"""
hedge_bot.py — Live Trading Bot: Hedge Dinamico BTC Up/Down 5m en Polymarket

Estrategia 100% fiel a hedge_sim.py v8 — Maker Entry / Taker Exit

CAMBIOS vs simulacion:
  - comprar()            -> coloca orden GTC BUY al ask (Maker Entry) y espera fill
  - intentar_early_exit()-> coloca orden GTC SELL al bid (Taker Exit, cruza inmediato)
  - Resolucion           -> identica a sim (Polymarket auto-liquida tokens ganadores)

VARIABLES DE ENTORNO (Railway):
  POLYMARKET_KEY     str    — clave privada de la wallet
  PROXY_ADDRESS      str    — direccion del proxy/funder
  CAPITAL_INICIAL    float  (default: 100.0)
  STATE_FILE         str    (default: /app/data/state.json)
  LOG_FILE           str    (default: /app/data/hedge_log.json)
  SYMBOL             str    (default: BTC)
"""

import asyncio
import os
import sys
import time
import json
import logging
from datetime import datetime, timezone
from collections import deque

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from strategy_core import (
    find_active_market,
    get_order_book_metrics,
    compute_signal,
    seconds_remaining,
)

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("hedge_bot")
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ─── CONFIG DESDE ENV VARS ────────────────────────────────────────────────────
POLYMARKET_KEY  = os.getenv("POLYMARKET_KEY")
PROXY_ADDRESS   = os.getenv("PROXY_ADDRESS")
CAPITAL_INICIAL = float(os.environ.get("CAPITAL_INICIAL", "100.0"))
STATE_FILE      = os.environ.get("STATE_FILE", "/app/data/state.json")
LOG_FILE        = os.environ.get("LOG_FILE",   "/app/data/hedge_log.json")
SYMBOL          = os.environ.get("SYMBOL", "BTC").upper()

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID  = 137

# ─── PARAMETROS (identicos a hedge_sim.py v8) ─────────────────────────────────
USD_POR_LADO         = 3.75    # monto fijo por lado (lado1 + lado2 = $7.50 por trade)

POLL_INTERVAL        = 1.0
ORDER_FILL_TIMEOUT   = 4.0     # segundos maximos esperando fill de entrada
ORDER_POLL_INTERVAL  = 2.0     # frecuencia de polling de estado de orden
MIN_FILL_SECS        = 1.0     # fill en menos de esto = mercado cayendo, salir inmediato

OBI_THRESHOLD        = 0.10
OBI_WINDOW_SIZE      = 8
OBI_STRONG_THRESHOLD = 0.20

SPREAD_MAX        = 0.12
PRECIO_MIN_LADO1  = 0.30    # v8: zona <0.30 es consistentemente perdedora
PRECIO_MAX_LADO1  = 0.75

ENTRY_WINDOW_MAX  = 240     # no entra si quedan mas de 240s
ENTRY_WINDOW_MIN  = 60      # no entra si quedan menos de 60s

HEDGE_MOVE_MIN    = 0.05    # lado1 debe subir 5c antes de hedgear
HEDGE_OBI_MIN     = -0.05
HEDGE_PRECIO_MIN  = 0.25    # v8: no hedgea si lado2 ya esta muy barato
HEDGE_PRECIO_MAX  = 0.35    # no hedgea si el lado2 ya esta caro

EARLY_EXIT_SECS       = 60     # sale si lleva 60s sin hedge
EARLY_EXIT_OBI_FLIP   = -0.15
EARLY_EXIT_PRICE_DROP = 0.08

RESOLVED_UP_THRESH = 0.97
RESOLVED_DN_THRESH = 0.03

MIN_USD_ORDEN = 1.00

# ─── CLIENTE CLOB ─────────────────────────────────────────────────────────────
clob = None

def init_clob():
    global clob
    if not POLYMARKET_KEY or not PROXY_ADDRESS:
        log.error("ERROR: POLYMARKET_KEY y PROXY_ADDRESS son requeridos como variables de entorno")
        sys.exit(1)
    clob = ClobClient(
        host=CLOB_HOST,
        key=POLYMARKET_KEY,
        chain_id=CHAIN_ID,
        funder=PROXY_ADDRESS,
        signature_type=1,
    )
    clob.set_api_creds(clob.create_or_derive_api_creds())
    try:
        clob.update_profile()
        log.info("Cliente CLOB autorizado (approvals on-chain OK).")
    except Exception as e:
        log.warning(f"update_profile no disponible o ya activo: {e}")
        log.info("Cliente CLOB autorizado.")

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
estado = {
    "capital":      CAPITAL_INICIAL,
    "capital_base": 0.0,          # primer balance real — base del ROI
    "pnl_total":    0.0,
    "peak_capital": CAPITAL_INICIAL,
    "max_drawdown": 0.0,
    "wins":         0,
    "losses":       0,
    "ciclos":       0,
    "trades":       [],
}

obi_history_up = deque(maxlen=OBI_WINDOW_SIZE)
obi_history_dn = deque(maxlen=OBI_WINDOW_SIZE)

pos = {
    "activa":           False,
    "lado1_side":       None,
    "lado1_precio":     0.0,
    "lado1_shares":     0.0,
    "lado1_usd":        0.0,
    "lado2_side":       None,
    "lado2_precio":     0.0,
    "lado2_shares":     0.0,
    "lado2_usd":        0.0,
    "hedgeado":         False,
    "en_salida":        False,   # True cuando hay early exit pendiente de confirmar
    "exit_razon":       None,    # razon del early exit (para el log)
    "exit_intentos":    0,       # cuantos reintentos de venta llevamos
    "lado1_token_id":   None,    # guardado al comprar — usado en todas las ventas
    "capital_usado":    0.0,
    "ts_entrada":       None,
    "secs_entrada":     0.0,
}

eventos      = deque(maxlen=100)
mkt_end_date = None
mkt_global   = None   # mercado activo (necesario para token_ids en ordenes)
bot_activo   = False  # arranca PAUSADO — el usuario activa desde el dashboard

BALANCE_UPDATE_INTERVAL = 600   # refrescar saldo real cada 10 minutos
_ts_ultimo_balance      = 0.0   # timestamp del ultimo fetch de saldo
_balance_real           = 0.0   # ultimo saldo USDC confirmado por el CLOB


# ─── PERSISTENCIA ─────────────────────────────────────────────────────────────

def guardar_estado(up_m=None, dn_m=None):
    total = estado["wins"] + estado["losses"]
    wr    = estado["wins"] / total * 100 if total > 0 else 0.0
    _base = estado["capital_base"] if estado["capital_base"] > 0 else CAPITAL_INICIAL
    roi   = (estado["capital"] - _base) / _base * 100

    ob_up = {
        "ask": round(up_m["best_ask"], 4),
        "bid": round(up_m["best_bid"], 4),
        "obi": round(up_m["obi"], 4),
    } if up_m else None

    ob_dn = {
        "ask": round(dn_m["best_ask"], 4),
        "bid": round(dn_m["best_bid"], 4),
        "obi": round(dn_m["obi"], 4),
    } if dn_m else None

    try:
        dirpath = os.path.dirname(STATE_FILE)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump({
                "ts":              datetime.now().isoformat(),
                "capital":         round(estado["capital"], 4),
                "capital_inicial": CAPITAL_INICIAL,
                "pnl_total":       round(estado["pnl_total"], 4),
                "roi":             round(roi, 2),
                "peak_capital":    round(estado["peak_capital"], 4),
                "max_drawdown":    round(estado["max_drawdown"], 4),
                "wins":            estado["wins"],
                "losses":          estado["losses"],
                "win_rate":        round(wr, 1),
                "ciclos":          estado["ciclos"],
                "ob_up":           ob_up,
                "ob_dn":           ob_dn,
                "posicion": {
                    "activa":        pos["activa"],
                    "lado1":         pos["lado1_side"],
                    "lado2":         pos["lado2_side"],
                    "hedgeado":      pos["hedgeado"],
                    "capital_usado": round(pos["capital_usado"], 4),
                },
                "mkt_end_date":   mkt_end_date,
                "bot_activo":     bot_activo,
                "balance_real":   round(_balance_real, 4),
                "balance_ts":     _ts_ultimo_balance,
                "eventos": list(eventos)[-30:],
                "trades":  estado["trades"][-20:],
            }, f, indent=2)
    except Exception as e:
        log.warning(f"guardar_estado error: {e}")

    try:
        dirpath = os.path.dirname(LOG_FILE)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        with open(LOG_FILE, "w") as f:
            json.dump({
                "summary": {
                    "capital_inicial": CAPITAL_INICIAL,
                    "capital_actual":  round(estado["capital"], 4),
                    "pnl_total":       round(estado["pnl_total"], 4),
                    "roi_pct":         round(roi, 2),
                    "max_drawdown":    round(estado["max_drawdown"], 4),
                    "wins":            estado["wins"],
                    "losses":          estado["losses"],
                    "win_rate":        round(wr, 1),
                },
                "trades": estado["trades"],
            }, f, indent=2)
    except Exception as e:
        log.warning(f"guardar_log error: {e}")


def restaurar_estado():
    if not os.path.isfile(LOG_FILE):
        log.info("Sin estado previo — iniciando desde cero.")
        return
    try:
        with open(LOG_FILE) as f:
            data = json.load(f)
        s = data.get("summary", {})
        estado["capital"]   = float(s.get("capital_actual", CAPITAL_INICIAL))
        estado["pnl_total"] = float(s.get("pnl_total", 0.0))
        estado["wins"]      = int(s.get("wins", 0))
        estado["losses"]    = int(s.get("losses", 0))
        estado["trades"]    = data.get("trades", [])

        peak = CAPITAL_INICIAL
        for t in estado["trades"]:
            cap = float(t.get("capital", CAPITAL_INICIAL))
            if cap > peak:
                peak = cap
            dd = peak - cap
            if dd > estado["max_drawdown"]:
                estado["max_drawdown"] = dd
        estado["peak_capital"] = peak

        total = estado["wins"] + estado["losses"]
        log.info(
            f"Estado restaurado — {total} trades | "
            f"Capital: ${estado['capital']:.2f} | "
            f"PnL: ${estado['pnl_total']:+.2f} | "
            f"W:{estado['wins']} L:{estado['losses']}"
        )
    except Exception as e:
        log.warning(f"No se pudo restaurar estado: {e}")


# ─── UTILIDADES ───────────────────────────────────────────────────────────────

def log_ev(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    eventos.append(f"[{ts}] {msg}")
    log.info(msg)

def mid(m) -> float:
    b, a = m["best_bid"], m["best_ask"]
    if b > 0 and a > 0:
        return round((b + a) / 2, 4)
    return round(b or a, 4)

def actualizar_drawdown():
    cap = estado["capital"]
    if cap > estado["peak_capital"]:
        estado["peak_capital"] = cap
    dd = estado["peak_capital"] - cap
    if dd > estado["max_drawdown"]:
        estado["max_drawdown"] = dd

def resetear_pos():
    for k in pos:
        if k in ("activa", "hedgeado", "en_salida"):
            pos[k] = False
        elif isinstance(pos[k], str):
            pos[k] = None
        else:
            pos[k] = 0.0

def imprimir_estado(up_m, dn_m, secs, signal_up, signal_dn):
    sep   = "-" * 65
    total = estado["wins"] + estado["losses"]
    wr    = estado["wins"] / total * 100 if total > 0 else 0
    _base = estado["capital_base"] if estado["capital_base"] > 0 else CAPITAL_INICIAL
    roi   = (estado["capital"] - _base) / _base * 100

    print(f"\n{sep}")
    print(f"  Capital: ${estado['capital']:.2f}  PnL: ${estado['pnl_total']:+.2f}  ROI: {roi:+.1f}%  MaxDD: ${estado['max_drawdown']:.2f}")
    print(f"  W:{estado['wins']} L:{estado['losses']} WR:{wr:.0f}%  |  Ciclos: {estado['ciclos']}")
    print(f"  Orden: ${USD_POR_LADO:.2f}/lado (fijo) = ${USD_POR_LADO*2:.2f} total/trade")

    if up_m and dn_m:
        print(f"  UP  bid={up_m['best_bid']:.3f} ask={up_m['best_ask']:.3f} mid={mid(up_m):.3f}  OBI={up_m['obi']:+.3f} spread={up_m['spread']:.3f}")
        print(f"  DN  bid={dn_m['best_bid']:.3f} ask={dn_m['best_ask']:.3f} mid={mid(dn_m):.3f}  OBI={dn_m['obi']:+.3f} spread={dn_m['spread']:.3f}")
        if signal_up:
            print(f"  Senal UP: {signal_up['label']} conf={signal_up['confidence']}%  combined={signal_up['combined']:+.3f}")
        if signal_dn:
            print(f"  Senal DN: {signal_dn['label']} conf={signal_dn['confidence']}%  combined={signal_dn['combined']:+.3f}")
        print(f"  Tiempo restante: {int(secs) if secs else '?'}s")

    if pos["activa"]:
        secs_en_pos = time.time() - pos["ts_entrada"] if pos["ts_entrada"] else 0
        print(f"\n  POSICION ABIERTA ({int(secs_en_pos)}s):")
        print(f"    Lado1: {pos['lado1_side']} @ {pos['lado1_precio']:.4f} | ${pos['lado1_usd']:.2f} | {pos['lado1_shares']:.4f}sh")
        if pos["hedgeado"]:
            print(f"    Lado2: {pos['lado2_side']} @ {pos['lado2_precio']:.4f} | ${pos['lado2_usd']:.2f} | {pos['lado2_shares']:.4f}sh")
            print(f"    Capital en juego: ${pos['capital_usado']:.2f} ({pos['capital_usado']/estado['capital']*100:.1f}%)")
        else:
            print(f"    Esperando hedge...")
    else:
        print(f"\n  Sin posicion abierta")
    print(sep)


# ─── COMPRA CON ORDEN REAL (Maker Entry — GTC al ask) ─────────────────────────

async def comprar_live(lado: str, token_id: str, ask: float, bid: float, loop) -> tuple[float, float, float]:
    """
    Maker Entry: coloca GTC BUY al bid+0.001 (fee_rate_bps=0, sin fee).
    El orden entra al libro como maker; si el mercado viene a ese precio se llena.
    Retorna (precio, shares, usd) o (0, 0, 0) si falla o timeout.
    """
    usd = USD_POR_LADO

    if usd < MIN_USD_ORDEN:
        log_ev(f"  x Orden muy pequena: ${usd:.2f} < minimo ${MIN_USD_ORDEN:.2f}")
        return 0.0, 0.0, 0.0

    if usd > estado["capital"]:
        log_ev(f"  x Capital insuficiente: ${estado['capital']:.2f}")
        return 0.0, 0.0, 0.0

    # Precio maker: bid + 0.001 (debajo del ask, entra al libro sin cruzar)
    maker_price = round(bid + 0.001, 4)
    shares      = round(usd / maker_price, 2)
    if shares < 5.0:
        shares = 5.0
    costo_real = round(shares * maker_price, 4)

    log_ev(f"  Colocando BUY maker {lado} @ {maker_price:.4f} (bid={bid:.4f} ask={ask:.4f}) | {shares} tokens | ${costo_real:.2f}")

    try:
        order_args   = OrderArgs(price=maker_price, size=shares, side=BUY,
                                 token_id=token_id, fee_rate_bps=1000)
        signed_order = await loop.run_in_executor(None, clob.create_order, order_args)
        resp         = await loop.run_in_executor(None, lambda: clob.post_order(signed_order, OrderType.GTC))

        if "orderID" not in resp:
            log_ev(f"  x API rechazo orden BUY: {resp}")
            return 0.0, 0.0, 0.0

        order_id = resp["orderID"]
        log_ev(f"  Orden BUY colocada: {order_id}")

    except Exception as e:
        log_ev(f"  x Error al colocar orden BUY: {e}")
        return 0.0, 0.0, 0.0

    # Esperar fill (el fill llega cuando un vendedor acepta nuestro precio)
    ts_orden = time.time()
    deadline = ts_orden + ORDER_FILL_TIMEOUT
    while time.time() < deadline:
        try:
            order_info = await loop.run_in_executor(None, clob.get_order, order_id)
            status = order_info.get("status", "") if order_info else ""
            if status in ("FILLED", "MATCHED"):
                fill_secs = time.time() - ts_orden
                if fill_secs < MIN_FILL_SECS:
                    # Fill demasiado rapido = alguien vendio agresivamente contra nosotros
                    log_ev(f"  ALERTA: fill en {fill_secs:.1f}s — mercado en caida, saliendo sin registrar posicion")
                    estado["capital"] -= costo_real
                    exit_precio = await vender_taker(lado, token_id, bid, shares, loop)
                    if exit_precio > 0.0:
                        estado["capital"] += shares * exit_precio
                        return 0.0, 0.0, 0.0
                    else:
                        # Venta fallida — restaurar capital y registrar posicion normal
                        estado["capital"] += costo_real
                        log_ev(f"  Fast-exit falló — registrando posicion para manejo normal")
                        log_ev(f"  FILL BUY {lado} @ {maker_price:.4f} | {shares} tokens | ${costo_real:.2f}")
                        estado["capital"] -= costo_real
                        return maker_price, shares, costo_real
                log_ev(f"  FILL BUY {lado} @ {maker_price:.4f} | {shares} tokens | ${costo_real:.2f}")
                estado["capital"] -= costo_real
                return maker_price, shares, costo_real
        except Exception as e:
            log_ev(f"  Advertencia al consultar orden: {e}")

        await asyncio.sleep(ORDER_POLL_INTERVAL)

    # Timeout — cancelar
    try:
        await loop.run_in_executor(None, clob.cancel, order_id)
        log_ev(f"  x Orden BUY cancelada por timeout ({ORDER_FILL_TIMEOUT:.0f}s)")
    except Exception as e:
        log_ev(f"  Advertencia al cancelar: {e}")

    return 0.0, 0.0, 0.0


# ─── VENTA TAKER (GTC al bid — cruza inmediatamente con el libro) ──────────────

async def vender_taker(lado: str, token_id: str, bid: float, shares: float, loop) -> float:
    """
    Coloca una orden GTC SELL al bid (Taker Exit — cruza con compradores existentes).
    Retorna el precio de ejecucion o 0.0 si falla.
    """
    exit_precio = max(round(bid, 4), 0.01)
    log_ev(f"  Colocando SELL taker {lado} @ {exit_precio:.4f} | {shares} tokens")

    try:
        order_args   = OrderArgs(price=exit_precio, size=shares, side=SELL,
                                 token_id=token_id, fee_rate_bps=1000)
        signed_order = await loop.run_in_executor(None, clob.create_order, order_args)
        resp         = await loop.run_in_executor(None, lambda: clob.post_order(signed_order, OrderType.GTC))

        if "orderID" not in resp:
            log_ev(f"  x API rechazo orden SELL: {resp}")
            return 0.0

        order_id = resp["orderID"]

        # Esperar fill (max 15s — debe cruzar rapido al bid)
        deadline = time.time() + 15.0
        while time.time() < deadline:
            try:
                order_info = await loop.run_in_executor(None, clob.get_order, order_id)
                status = order_info.get("status", "")
                if status in ("FILLED", "MATCHED"):
                    log_ev(f"  FILL SELL {lado} @ {exit_precio:.4f}")
                    return exit_precio
            except Exception:
                pass
            await asyncio.sleep(1.0)

        # No lleno — cancelar
        try:
            await loop.run_in_executor(None, clob.cancel, order_id)
        except Exception:
            pass
        log_ev(f"  x Orden SELL no lleno en 15s — posicion cerrada a 0")
        return 0.0

    except Exception as e:
        log_ev(f"  x Error en venta taker: {e}")
        return 0.0


# ─── SENAL DE ENTRADA (identica a hedge_sim.py v8) ────────────────────────────

def evaluar_senal(up_m, dn_m):
    obi_up = up_m["obi"]
    obi_dn = dn_m["obi"]
    obi_history_up.append(obi_up)
    obi_history_dn.append(obi_dn)

    signal_up = compute_signal(obi_up, list(obi_history_up), OBI_THRESHOLD)
    signal_dn = compute_signal(obi_dn, list(obi_history_dn), OBI_THRESHOLD)

    if up_m["spread"] > SPREAD_MAX or dn_m["spread"] > SPREAD_MAX:
        return signal_up, signal_dn, None

    # Verificar precio maker real (bid+0.001), no el ask
    # Evita entrar en zona <PRECIO_MIN cuando el spread es amplio
    maker_up = round(up_m["best_bid"] + 0.001, 4)
    maker_dn = round(dn_m["best_bid"] + 0.001, 4)

    if signal_up["combined"] >= OBI_STRONG_THRESHOLD:
        if PRECIO_MIN_LADO1 <= maker_up <= PRECIO_MAX_LADO1:
            return signal_up, signal_dn, "UP"

    if signal_dn["combined"] >= OBI_STRONG_THRESHOLD:
        if PRECIO_MIN_LADO1 <= maker_dn <= PRECIO_MAX_LADO1:
            return signal_up, signal_dn, "DOWN"

    if signal_up["label"] in ("UP", "STRONG UP") and signal_up["combined"] > signal_dn["combined"]:
        if PRECIO_MIN_LADO1 <= maker_up <= PRECIO_MAX_LADO1:
            return signal_up, signal_dn, "UP"

    if signal_dn["label"] in ("UP", "STRONG UP") and signal_dn["combined"] > signal_up["combined"]:
        if PRECIO_MIN_LADO1 <= maker_dn <= PRECIO_MAX_LADO1:
            return signal_up, signal_dn, "DOWN"

    return signal_up, signal_dn, None


# ─── ENTRADA LADO 1 ───────────────────────────────────────────────────────────

async def intentar_entrada(up_m, dn_m, secs, loop) -> bool:
    if not bot_activo:
        return False
    if pos["activa"]:
        return False
    if secs is None or not (ENTRY_WINDOW_MIN < secs <= ENTRY_WINDOW_MAX):
        return False

    signal_up, signal_dn, lado = evaluar_senal(up_m, dn_m)
    if not lado:
        return False

    ask      = up_m["best_ask"] if lado == "UP" else dn_m["best_ask"]
    bid      = up_m["best_bid"] if lado == "UP" else dn_m["best_bid"]
    obi      = up_m["obi"]      if lado == "UP" else dn_m["obi"]
    token_id = mkt_global["up_token_id"] if lado == "UP" else mkt_global["down_token_id"]

    log_ev(f"SENAL {lado} — OBI={obi:+.3f} | bid={bid:.4f} ask={ask:.4f} | {int(secs)}s restantes")

    precio, shares, usd = await comprar_live(lado, token_id, ask, bid, loop)
    if usd == 0.0:
        return False

    pos["activa"]          = True
    pos["lado1_side"]      = lado
    pos["lado1_token_id"]  = token_id
    pos["lado1_precio"]    = precio
    pos["lado1_shares"]    = shares
    pos["lado1_usd"]       = usd
    pos["capital_usado"]   = usd
    pos["ts_entrada"]      = time.time()
    pos["secs_entrada"]    = secs or 0

    log_ev(f"ENTRADA LADO1 {lado} @ {precio:.4f} | {shares:.4f}sh | ${usd:.2f} | cap=${estado['capital']:.2f}")
    guardar_estado(up_m, dn_m)
    return True


# ─── HEDGE LADO 2 (identico a hedge_sim.py v8, con orden real) ────────────────

async def intentar_hedge(up_m, dn_m, loop):
    if not pos["activa"] or pos["hedgeado"]:
        return

    lado1     = pos["lado1_side"]
    lado2     = "DOWN" if lado1 == "UP" else "UP"
    bid_lado1 = up_m["best_bid"] if lado1 == "UP" else dn_m["best_bid"]
    subida    = bid_lado1 - pos["lado1_precio"]

    if subida < HEDGE_MOVE_MIN:
        return

    obi_lado2 = dn_m["obi"] if lado2 == "DOWN" else up_m["obi"]
    if obi_lado2 < HEDGE_OBI_MIN:
        return

    ask_lado2 = dn_m["best_ask"] if lado2 == "DOWN" else up_m["best_ask"]
    bid_lado2 = dn_m["best_bid"] if lado2 == "DOWN" else up_m["best_bid"]
    token_id  = mkt_global["down_token_id"] if lado2 == "DOWN" else mkt_global["up_token_id"]

    # v8: rango optimo del hedge 0.25-0.35
    if ask_lado2 <= 0 or ask_lado2 < HEDGE_PRECIO_MIN or ask_lado2 > HEDGE_PRECIO_MAX:
        return

    log_ev(f"  Lado1 subio {subida*100:+.1f}c — hedgeando en {lado2} @ bid={bid_lado2:.4f} ask={ask_lado2:.4f}")

    precio, shares, usd = await comprar_live(lado2, token_id, ask_lado2, bid_lado2, loop)
    if usd == 0.0:
        return

    pos["lado2_side"]    = lado2
    pos["lado2_precio"]  = precio
    pos["lado2_shares"]  = shares
    pos["lado2_usd"]     = usd
    pos["hedgeado"]      = True
    pos["capital_usado"] += usd

    log_ev(f"HEDGE LADO2 {lado2} @ {precio:.4f} | {shares:.4f}sh | ${usd:.2f} | cap=${estado['capital']:.2f}")
    guardar_estado(up_m, dn_m)


# ─── SALIDA ANTICIPADA (Taker Exit — GTC al bid) ──────────────────────────────

async def intentar_early_exit(up_m, dn_m, loop):
    if not pos["activa"] or pos["hedgeado"]:
        return

    lado1       = pos["lado1_side"]
    bid_lado1   = up_m["best_bid"] if lado1 == "UP" else dn_m["best_bid"]
    obi_lado1   = up_m["obi"]      if lado1 == "UP" else dn_m["obi"]
    token_id    = pos["lado1_token_id"]   # token guardado al comprar, no depende de mkt_global
    secs_en_pos = time.time() - pos["ts_entrada"] if pos["ts_entrada"] else 0
    caida       = pos["lado1_precio"] - bid_lado1

    razon = None
    if secs_en_pos > EARLY_EXIT_SECS:
        razon = f"timeout {int(secs_en_pos)}s sin hedge"
    elif obi_lado1 < EARLY_EXIT_OBI_FLIP:
        razon = f"OBI invertido {obi_lado1:+.3f}"
    elif caida > EARLY_EXIT_PRICE_DROP:
        razon = f"caida {caida*100:.1f}c desde entrada"

    if not razon:
        return

    log_ev(f"EARLY EXIT {lado1} — {razon} — vendiendo al bid {bid_lado1:.4f}")

    exit_precio = await vender_taker(lado1, token_id, bid_lado1, pos["lado1_shares"], loop)
    if exit_precio == 0.0:
        # Venta fallida — marcar para reintento continuo, NO abandonar la posicion
        pos["en_salida"]     = True
        pos["exit_razon"]    = razon
        pos["exit_intentos"] = 1
        log_ev(f"  EARLY EXIT: primer intento fallido — reintentando cada tick hasta confirmar venta")
        guardar_estado(up_m, dn_m)
        return

    _cerrar_early_exit(lado1, exit_precio, razon, up_m, dn_m)


# ─── CIERRE DE EARLY EXIT (helper compartido) ────────────────────────────────

def _cerrar_early_exit(lado1: str, exit_precio: float, razon: str, up_m, dn_m):
    """Contabiliza y registra un early exit confirmado (sell llenado)."""
    pnl = round(pos["lado1_shares"] * exit_precio - pos["lado1_usd"], 4)

    estado["capital"]   += pos["lado1_usd"] + pnl
    estado["pnl_total"] += pnl

    if pnl >= 0:
        estado["wins"] += 1
    else:
        estado["losses"] += 1

    actualizar_drawdown()
    log_ev(f"EARLY EXIT {lado1} @ {exit_precio:.4f} | {razon} | PnL: ${pnl:+.4f} | cap=${estado['capital']:.2f}")
    _registrar_trade("EARLY_EXIT", exit_precio, None, "WIN" if pnl >= 0 else "LOSS", pnl)
    resetear_pos()
    guardar_estado(up_m, dn_m)


# ─── REINTENTO PERSISTENTE DE VENTA ──────────────────────────────────────────

async def forzar_salida(up_m, dn_m, loop):
    """
    Llamado cada tick cuando pos["en_salida"]=True.
    Reintenta la venta con precio cada vez mas agresivo hasta confirmar el fill.
    Bajo ningun concepto resetea la posicion sin haber vendido primero.

    Escalado de precio:
      intentos 1-3  : bid actual           (taker normal)
      intentos 4-6  : bid - 0.02           (ligeramente agresivo)
      intentos 7+   : bid - 0.05 (min 0.01) (dump total — garantiza fill)
    """
    lado1    = pos["lado1_side"]
    intentos = pos["exit_intentos"]
    bid_raw  = up_m["best_bid"] if lado1 == "UP" else dn_m["best_bid"]
    token_id = pos["lado1_token_id"]   # token guardado al comprar, no depende de mkt_global

    if intentos <= 3:
        precio_exit = max(round(bid_raw, 4), 0.01)
    elif intentos <= 6:
        precio_exit = max(round(bid_raw - 0.02, 4), 0.01)
    else:
        precio_exit = max(round(bid_raw - 0.05, 4), 0.01)

    # Solo loguear intento #1, multiples de 5, y cuando cambia el escalon de precio
    if intentos == 1 or intentos % 5 == 0:
        log_ev(f"  REINTENTO VENTA #{intentos} {lado1} @ {precio_exit:.4f} (bid={bid_raw:.4f})")

    # Alerta si llevamos demasiados reintentos sin exito
    if intentos == 15:
        log_ev(f"  ALERTA: {intentos} reintentos fallidos — posible problema de allowance on-chain")

    # Backoff a partir del intento 10: esperar mas entre intentos para no saturar la API
    if intentos >= 10:
        await asyncio.sleep(3.0)

    pos["exit_intentos"] += 1
    exit_precio = await vender_taker(lado1, token_id, precio_exit, pos["lado1_shares"], loop)

    if exit_precio == 0.0:
        if intentos <= 3 or intentos % 5 == 0:
            log_ev(f"  x Reintento #{intentos} fallido")
        guardar_estado(up_m, dn_m)
        return

    razon = pos["exit_razon"] or "early_exit"
    log_ev(f"  Venta confirmada en reintento #{intentos}")
    _cerrar_early_exit(lado1, exit_precio, razon, up_m, dn_m)


# ─── RESOLUCION (identica a hedge_sim.py — Polymarket auto-liquida los ganadores)

def verificar_resolucion(up_m, dn_m, secs):
    if not pos["activa"]:
        return

    up_mid = mid(up_m)
    dn_mid = mid(dn_m)

    resuelto = None
    if up_mid >= RESOLVED_UP_THRESH:
        resuelto = "UP"
    elif up_mid <= RESOLVED_DN_THRESH:
        resuelto = "DOWN"
    elif dn_mid >= RESOLVED_UP_THRESH:
        resuelto = "DOWN"
    elif secs is not None and secs <= 0:
        resuelto = "UP" if up_mid > 0.5 else "DOWN"
        log_ev(f"Tiempo agotado — resolviendo por mid UP={up_mid:.3f} -> {resuelto}")

    if resuelto:
        _aplicar_resolucion(resuelto)


def _aplicar_resolucion(resuelto: str):
    pnl_total = 0.0
    partes    = []

    if resuelto == pos["lado1_side"]:
        pnl_l1 = pos["lado1_shares"] * 1.0 - pos["lado1_usd"]
        partes.append(f"L1 {pos['lado1_side']}=WIN(${pnl_l1:+.2f})")
    else:
        pnl_l1 = -pos["lado1_usd"]
        partes.append(f"L1 {pos['lado1_side']}=LOSS(${pnl_l1:+.2f})")
    pnl_total += pnl_l1

    if pos["hedgeado"]:
        if resuelto == pos["lado2_side"]:
            pnl_l2 = pos["lado2_shares"] * 1.0 - pos["lado2_usd"]
            partes.append(f"L2 {pos['lado2_side']}=WIN(${pnl_l2:+.2f})")
        else:
            pnl_l2 = -pos["lado2_usd"]
            partes.append(f"L2 {pos['lado2_side']}=LOSS(${pnl_l2:+.2f})")
        pnl_total += pnl_l2

    estado["capital"]   += pos["capital_usado"] + pnl_total
    estado["pnl_total"] += pnl_total

    outcome = "WIN" if pnl_total >= 0 else "LOSS"
    if outcome == "WIN":
        estado["wins"] += 1
    else:
        estado["losses"] += 1

    actualizar_drawdown()
    log_ev(
        f"RESOLUCION -> {resuelto} | {' | '.join(partes)} | "
        f"PnL NETO: ${pnl_total:+.2f} | cap=${estado['capital']:.2f}"
    )
    _registrar_trade("RESOLUTION", 1.0 if resuelto == pos["lado1_side"] else 0.0, resuelto, outcome, pnl_total)
    resetear_pos()
    guardar_estado()


def _registrar_trade(tipo, exit_precio, resuelto, outcome, pnl):
    estado["trades"].append({
        "ts":           datetime.now().isoformat(),
        "tipo":         tipo,
        "resolucion":   resuelto,
        "lado1_side":   pos["lado1_side"],
        "lado1_usd":    round(pos["lado1_usd"], 4),
        "lado1_precio": round(pos["lado1_precio"], 4),
        "hedgeado":     pos["hedgeado"],
        "lado2_side":   pos["lado2_side"],
        "lado2_usd":    round(pos["lado2_usd"], 4),
        "lado2_precio": round(pos["lado2_precio"], 4),
        "exit_precio":  round(exit_precio, 4),
        "pnl":          round(pnl, 4),
        "capital":      round(estado["capital"], 4),
        "outcome":      outcome,
    })


# ─── LOOP PRINCIPAL ───────────────────────────────────────────────────────────

async def main_loop():
    global mkt_global, mkt_end_date, bot_activo, _ts_ultimo_balance

    log_ev("=" * 65)
    log_ev(f"  HEDGE BOT LIVE v8 — {SYMBOL} Up/Down 5m en Polymarket")
    log_ev(f"  Capital: ${CAPITAL_INICIAL:.0f} | Orden fija: ${USD_POR_LADO:.2f}/lado = ${USD_POR_LADO*2:.2f}/trade")
    log_ev(f"  Entrada: precio [{PRECIO_MIN_LADO1:.2f}-{PRECIO_MAX_LADO1:.2f}]")
    log_ev(f"  Hedge:   precio [{HEDGE_PRECIO_MIN:.2f}-{HEDGE_PRECIO_MAX:.2f}] | move_min={HEDGE_MOVE_MIN:.2f}")
    log_ev(f"  Modo: Maker Entry / Taker Exit")
    log_ev("=" * 65)

    restaurar_estado()
    guardar_estado()

    loop              = asyncio.get_running_loop()
    signal_up_cache   = None
    signal_dn_cache   = None
    ya_opero_ciclo    = False
    _ob_errores       = 0     # contador de errores OB consecutivos para backoff
    _bot_prev_activo  = False # detectar transicion pausa→activo
    _skip_primer_ciclo = False # True tras activacion: saltarse el primer ciclo que se encuentre

    while True:
        try:
            # 0. Si el bot esta pausado, solo guardar estado y esperar
            if not bot_activo:
                _bot_prev_activo   = False
                _skip_primer_ciclo = False
                guardar_estado()
                await asyncio.sleep(2)
                continue

            # 0c. Detectar transicion pausa→activo: marcar para saltar el primer ciclo
            if not _bot_prev_activo:
                _skip_primer_ciclo = True
                if mkt_global is not None:
                    # Ya hay mercado en curso — marcarlo como saltado inmediatamente
                    ya_opero_ciclo = True
                    log_ev("Bot activado — saltando ciclo en curso, esperando el proximo")
                else:
                    log_ev("Bot activado — saltando primer ciclo que se encuentre")
            _bot_prev_activo = True

            # 0b. Refrescar saldo real cada 10 minutos
            if time.time() - _ts_ultimo_balance > BALANCE_UPDATE_INTERVAL:
                await loop.run_in_executor(None, _refrescar_balance_real)

            # 1. Descubrir mercado
            if mkt_global is None:
                log_ev(f"Buscando mercado {SYMBOL} Up/Down 5m...")
                guardar_estado()
                obi_history_up.clear()
                obi_history_dn.clear()
                ya_opero_ciclo = False
                mkt = await loop.run_in_executor(None, find_active_market, SYMBOL)
                if mkt:
                    mkt_global = mkt
                    estado["ciclos"] += 1
                    mkt_end_date = mkt.get("end_date")
                    # Si el bot acaba de activarse, saltar este primer ciclo
                    if _skip_primer_ciclo:
                        ya_opero_ciclo     = True
                        _skip_primer_ciclo = False
                        log_ev(f"Mercado encontrado (saltado): {mkt.get('question','')} — siguiente ciclo sera el primero")
                    else:
                        log_ev(f"Mercado: {mkt.get('question','')}")
                    guardar_estado()
                else:
                    log_ev("Sin mercado activo — reintentando en 10s...")
                    guardar_estado()
                    await asyncio.sleep(10)
                    continue

            # 2. Leer order books
            up_m, err_up = await loop.run_in_executor(
                None, get_order_book_metrics, mkt_global["up_token_id"]
            )
            dn_m, err_dn = await loop.run_in_executor(
                None, get_order_book_metrics, mkt_global["down_token_id"]
            )

            if not up_m or not dn_m:
                _ob_errores += 1
                espera = min(2 ** _ob_errores, 30)
                err_msg = str(err_up or err_dn)
                # Mensaje corto: solo codigo de status si es 429
                if "429" in err_msg:
                    log_ev(f"  429 rate-limit (#{_ob_errores}) — backoff {espera}s")
                else:
                    log_ev(f"  Error OB (#{_ob_errores}): {err_msg[:80]} — backoff {espera}s")
                await asyncio.sleep(espera)
                continue

            if _ob_errores > 0:
                log_ev(f"  OB recuperado tras {_ob_errores} error(es)")
            _ob_errores = 0

            secs = seconds_remaining(mkt_global)

            # 3. Mercado expirado
            if secs is not None and secs <= 0:
                if pos["activa"]:
                    verificar_resolucion(up_m, dn_m, secs)
                log_ev("Mercado expirado — buscando proximo ciclo...")
                mkt_global   = None
                mkt_end_date = None
                await asyncio.sleep(5)
                continue

            # 4. Resolucion por precio concluyente
            if pos["activa"]:
                verificar_resolucion(up_m, dn_m, secs)

            # 5. Reintento persistente de venta si hay early exit pendiente
            if pos["en_salida"]:
                await forzar_salida(up_m, dn_m, loop)

            # 6. Early exit si no hay hedge (solo si no estamos ya intentando salir)
            if pos["activa"] and not pos["hedgeado"] and not pos["en_salida"]:
                await intentar_early_exit(up_m, dn_m, loop)

            # 7. Intentar hedge (solo si no estamos intentando salir)
            if pos["activa"] and not pos["hedgeado"] and not pos["en_salida"]:
                await intentar_hedge(up_m, dn_m, loop)

            # 8. Nueva entrada — maximo una por ciclo de mercado
            if not pos["activa"] and not ya_opero_ciclo:
                if await intentar_entrada(up_m, dn_m, secs, loop):
                    ya_opero_ciclo = True

            # 9. Senales para display
            signal_up_cache = compute_signal(up_m["obi"], list(obi_history_up), OBI_THRESHOLD)
            signal_dn_cache = compute_signal(dn_m["obi"], list(obi_history_dn), OBI_THRESHOLD)

            # 10. Guardar estado con OB actualizado cada tick
            guardar_estado(up_m, dn_m)

            # 11. Mostrar en logs
            imprimir_estado(up_m, dn_m, secs, signal_up_cache, signal_dn_cache)

        except Exception as e:
            log_ev(f"Error en loop: {e}")
            import traceback
            traceback.print_exc()

        await asyncio.sleep(POLL_INTERVAL)


# ─── PRUEBA DE 3 CICLOS — ESTADO GLOBAL ──────────────────────────────────────

_test_estado = {
    "status":     "idle",   # idle | running | done | error
    "ciclos":     [],
    "ciclo_actual": 0,
    "started_at": None,
    "finished_at": None,
    "pnl_total_simulado": 0.0,
    "resumen_final": "",
    "error": None,
}
_test_thread = None

TEST_POLL_SECS   = 10   # snapshot cada 10s dentro de cada ciclo
NUM_CICLOS_TEST  = 3


def _run_test_3ciclos():
    """
    Corre en un thread separado. Observa NUM_CICLOS_TEST mercados consecutivos
    SIN colocar ordenes reales. Simula toda la logica de entrada, hedge, early
    exit y resolucion con el mismo codigo de decision que el bot live.
    """
    global _test_estado
    _test_estado = {
        "status": "running", "ciclos": [], "ciclo_actual": 0,
        "started_at": datetime.now().isoformat(), "finished_at": None,
        "pnl_total_simulado": 0.0, "resumen_final": "", "error": None,
    }

    try:
        # ── Pre-chequeo: auth + saldo ────────────────────────────────────────
        try:
            orders = clob.get_orders()
            _test_estado["clob_ok"] = True
            _test_estado["ordenes_abiertas"] = len(orders or [])
        except Exception as e:
            _test_estado["status"] = "error"
            _test_estado["error"]  = f"Auth CLOB fallo: {e}"
            return

        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            ba  = clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            bal = float(ba.get("balance", 0)) / 1_000_000
            _test_estado["saldo_usdc"] = round(bal, 2)
        except Exception:
            _test_estado["saldo_usdc"] = None

        # ── Orden de prueba real (place + cancel) ────────────────────────────
        _test_estado["orden_prueba"] = {"status": "pendiente"}
        try:
            mkt_p = find_active_market(SYMBOL)
            if mkt_p:
                up_p, _ = get_order_book_metrics(mkt_p["up_token_id"])
                if up_p and up_p["best_bid"] > 0:
                    tp    = round(up_p["best_bid"] + 0.001, 4)
                    tsize = 5.0
                    oa    = OrderArgs(price=tp, size=tsize, side=BUY,
                                     token_id=mkt_p["up_token_id"], fee_rate_bps=1000)
                    so   = clob.create_order(oa)
                    resp = clob.post_order(so, OrderType.GTC)
                    if "orderID" in resp:
                        time.sleep(0.5)
                        clob.cancel(resp["orderID"])
                        _test_estado["orden_prueba"] = {
                            "status": "ok", "order_id": resp["orderID"],
                            "precio": tp, "size": tsize,
                            "accion": "COLOCADA y CANCELADA (sin costo real)",
                        }
                    else:
                        _test_estado["orden_prueba"] = {"status": "error", "resp": str(resp)}
        except Exception as e:
            _test_estado["orden_prueba"] = {"status": "error", "error": str(e)}

        # ── 3 ciclos de observacion ──────────────────────────────────────────
        for ciclo_num in range(1, NUM_CICLOS_TEST + 1):
            _test_estado["ciclo_actual"] = ciclo_num
            ciclo = {
                "num": ciclo_num, "status": "buscando_mercado",
                "mercado": None, "snapshots": [],
                "sim_entrada": None, "sim_hedge": None, "sim_exit": None,
                "resolucion_detectada": None, "pnl_simulado": 0.0, "resumen": "",
            }
            _test_estado["ciclos"].append(ciclo)

            # Buscar mercado — esperar hasta 5 min si estamos entre slots
            log.info(f"[TEST] Ciclo {ciclo_num}/{NUM_CICLOS_TEST} — buscando mercado {SYMBOL}...")
            mkt = None
            for _ in range(30):
                mkt = find_active_market(SYMBOL)
                if mkt:
                    break
                time.sleep(10)

            if not mkt:
                ciclo["status"]  = "error"
                ciclo["resumen"] = "No se encontro mercado activo tras 5 min de espera"
                continue

            secs_start = seconds_remaining(mkt) or 0
            ciclo["mercado"] = {
                "question":           mkt.get("question", ""),
                "end_date":           mkt.get("end_date", ""),
                "secs_al_descubrir":  round(secs_start, 1),
                "up_token":           mkt["up_token_id"][:16] + "...",
                "down_token":         mkt["down_token_id"][:16] + "...",
            }
            ciclo["status"] = "observando"
            log.info(f"[TEST] Ciclo {ciclo_num} — {mkt.get('question','')} | {secs_start:.0f}s restantes")

            # Estado simulado de posicion (no toca capital real)
            obi_hist_up, obi_hist_dn = [], []
            sim = {
                "activa": False, "lado1": None, "l1_precio": 0.0,
                "l1_shares": 0.0, "hedgeado": False,
                "lado2": None, "l2_precio": 0.0, "l2_shares": 0.0,
                "ts_entrada": None,
            }
            ya_opero = False

            # Loop de observacion del ciclo
            while True:
                up_m, _ = get_order_book_metrics(mkt["up_token_id"])
                dn_m, _ = get_order_book_metrics(mkt["down_token_id"])
                secs    = seconds_remaining(mkt)

                if not up_m or not dn_m:
                    time.sleep(TEST_POLL_SECS)
                    continue

                obi_up = up_m["obi"]
                obi_dn = dn_m["obi"]
                obi_hist_up = (obi_hist_up + [obi_up])[-8:]
                obi_hist_dn = (obi_hist_dn + [obi_dn])[-8:]

                sig_up = compute_signal(obi_up, obi_hist_up, OBI_THRESHOLD)
                sig_dn = compute_signal(obi_dn, obi_hist_dn, OBI_THRESHOLD)

                in_ventana = ENTRY_WINDOW_MIN < (secs or 0) <= ENTRY_WINDOW_MAX
                spread_ok  = up_m["spread"] <= SPREAD_MAX and dn_m["spread"] <= SPREAD_MAX
                up_mid_v   = (up_m["best_bid"] + up_m["best_ask"]) / 2
                dn_mid_v   = (dn_m["best_bid"] + dn_m["best_ask"]) / 2

                # ── Evaluar entrada simulada ─────────────────────────────────
                would_enter = None
                if not ya_opero and in_ventana and spread_ok:
                    if sig_up["combined"] >= OBI_STRONG_THRESHOLD and PRECIO_MIN_LADO1 <= up_m["best_ask"] <= PRECIO_MAX_LADO1:
                        would_enter = "UP"
                    elif sig_dn["combined"] >= OBI_STRONG_THRESHOLD and PRECIO_MIN_LADO1 <= dn_m["best_ask"] <= PRECIO_MAX_LADO1:
                        would_enter = "DOWN"
                    elif sig_up["label"] in ("UP", "STRONG UP") and sig_up["combined"] > sig_dn["combined"] and PRECIO_MIN_LADO1 <= up_m["best_ask"] <= PRECIO_MAX_LADO1:
                        would_enter = "UP"
                    elif sig_dn["label"] in ("UP", "STRONG UP") and sig_dn["combined"] > sig_up["combined"] and PRECIO_MIN_LADO1 <= dn_m["best_ask"] <= PRECIO_MAX_LADO1:
                        would_enter = "DOWN"

                if would_enter and not sim["activa"] and not ya_opero:
                    ask = up_m["best_ask"] if would_enter == "UP" else dn_m["best_ask"]
                    bid = up_m["best_bid"] if would_enter == "UP" else dn_m["best_bid"]
                    sim.update({
                        "activa": True, "lado1": would_enter,
                        "l1_precio": ask,
                        "l1_shares": round(USD_POR_LADO / ask, 4),
                        "ts_entrada": time.time(),
                    })
                    ya_opero = True
                    ciclo["sim_entrada"] = {
                        "lado": would_enter, "ask": ask, "bid": bid,
                        "shares": sim["l1_shares"], "usd": USD_POR_LADO,
                        "obi": round(obi_up if would_enter == "UP" else obi_dn, 4),
                        "sig_combined": sig_up["combined"] if would_enter == "UP" else sig_dn["combined"],
                        "secs_restantes": round(secs or 0, 1),
                    }

                # ── Evaluar hedge simulado ───────────────────────────────────
                if sim["activa"] and not sim["hedgeado"]:
                    bid_l1  = up_m["best_bid"] if sim["lado1"] == "UP" else dn_m["best_bid"]
                    subida  = bid_l1 - sim["l1_precio"]
                    lado2   = "DOWN" if sim["lado1"] == "UP" else "UP"
                    ask_l2  = dn_m["best_ask"] if lado2 == "DOWN" else up_m["best_ask"]
                    obi_l2  = dn_m["obi"]      if lado2 == "DOWN" else up_m["obi"]

                    if subida >= HEDGE_MOVE_MIN and obi_l2 >= HEDGE_OBI_MIN and HEDGE_PRECIO_MIN <= ask_l2 <= HEDGE_PRECIO_MAX:
                        sim.update({
                            "hedgeado": True, "lado2": lado2,
                            "l2_precio": ask_l2,
                            "l2_shares": round(USD_POR_LADO / ask_l2, 4),
                        })
                        ciclo["sim_hedge"] = {
                            "lado": lado2, "ask": ask_l2,
                            "shares": sim["l2_shares"], "usd": USD_POR_LADO,
                            "subida_l1_cents": round(subida * 100, 2),
                            "obi_l2": round(obi_l2, 4),
                            "secs_restantes": round(secs or 0, 1),
                        }

                # ── Evaluar early exit simulado ──────────────────────────────
                if sim["activa"] and not sim["hedgeado"] and not ciclo["sim_exit"]:
                    bid_l1      = up_m["best_bid"] if sim["lado1"] == "UP" else dn_m["best_bid"]
                    obi_l1      = up_m["obi"]      if sim["lado1"] == "UP" else dn_m["obi"]
                    caida       = sim["l1_precio"] - bid_l1
                    secs_en_pos = time.time() - sim["ts_entrada"] if sim["ts_entrada"] else 0
                    razon_exit  = None
                    if secs_en_pos > EARLY_EXIT_SECS:
                        razon_exit = f"timeout {int(secs_en_pos)}s sin hedge"
                    elif obi_l1 < EARLY_EXIT_OBI_FLIP:
                        razon_exit = f"OBI invertido {obi_l1:+.3f}"
                    elif caida > EARLY_EXIT_PRICE_DROP:
                        razon_exit = f"caida {caida*100:.1f}c desde entrada"
                    if razon_exit:
                        pnl_exit = round(sim["l1_shares"] * bid_l1 - USD_POR_LADO, 4)
                        ciclo["sim_exit"] = {
                            "razon": razon_exit, "precio_exit": bid_l1,
                            "pnl_sim": pnl_exit, "secs_restantes": round(secs or 0, 1),
                        }
                        sim["activa"] = False

                # ── Detectar resolucion ──────────────────────────────────────
                resuelto = None
                if   up_mid_v >= RESOLVED_UP_THRESH: resuelto = "UP"
                elif up_mid_v <= RESOLVED_DN_THRESH:  resuelto = "DOWN"
                elif dn_mid_v >= RESOLVED_UP_THRESH:  resuelto = "DOWN"

                # ── Snapshot ─────────────────────────────────────────────────
                ciclo["snapshots"].append({
                    "ts":          datetime.now().strftime("%H:%M:%S"),
                    "secs":        round(secs or 0, 1),
                    "UP":  {"bid": up_m["best_bid"], "ask": up_m["best_ask"],
                            "obi": round(obi_up, 4), "spread": up_m["spread"],
                            "bid_vol": up_m["bid_volume"], "ask_vol": up_m["ask_volume"]},
                    "DN":  {"bid": dn_m["best_bid"], "ask": dn_m["best_ask"],
                            "obi": round(obi_dn, 4), "spread": dn_m["spread"],
                            "bid_vol": dn_m["bid_volume"], "ask_vol": dn_m["ask_volume"]},
                    "sig_up": {"label": sig_up["label"], "combined": sig_up["combined"]},
                    "sig_dn": {"label": sig_dn["label"], "combined": sig_dn["combined"]},
                    "ventana_ok":   in_ventana,
                    "spread_ok":    spread_ok,
                    "would_enter":  would_enter,
                    "sim_activa":   sim["activa"],
                    "sim_hedgeado": sim["hedgeado"],
                    "resolucion":   resuelto,
                })

                if resuelto:
                    ciclo["resolucion_detectada"] = resuelto

                # Salir del loop si el mercado expiro o se resolvio
                if secs is not None and secs <= 0:
                    break
                if resuelto:
                    time.sleep(TEST_POLL_SECS)
                    break

                time.sleep(TEST_POLL_SECS)

            # ── Calcular PnL simulado del ciclo ──────────────────────────────
            res_c = ciclo["resolucion_detectada"]
            if ciclo["sim_exit"]:
                ciclo["pnl_simulado"] = ciclo["sim_exit"]["pnl_sim"]
                ciclo["resumen"] = f"Early exit ({ciclo['sim_exit']['razon']}) | PnL sim: ${ciclo['pnl_simulado']:+.2f}"

            elif ciclo["sim_entrada"] and res_c:
                lado1  = ciclo["sim_entrada"]["lado"]
                sh1    = ciclo["sim_entrada"]["shares"]
                pnl1   = round(sh1 * 1.0 - USD_POR_LADO, 4) if res_c == lado1 else -USD_POR_LADO
                r1txt  = f"L1 {lado1} {'WIN' if res_c==lado1 else 'LOSS'} ${pnl1:+.2f}"
                pnl_t  = pnl1

                r2txt = ""
                if ciclo["sim_hedge"]:
                    lado2 = ciclo["sim_hedge"]["lado"]
                    sh2   = ciclo["sim_hedge"]["shares"]
                    pnl2  = round(sh2 * 1.0 - USD_POR_LADO, 4) if res_c == lado2 else -USD_POR_LADO
                    r2txt = f" | L2 {lado2} {'WIN' if res_c==lado2 else 'LOSS'} ${pnl2:+.2f}"
                    pnl_t += pnl2

                ciclo["pnl_simulado"] = round(pnl_t, 4)
                ciclo["resumen"] = f"Resolucion={res_c} | {r1txt}{r2txt} | PnL sim: ${pnl_t:+.2f}"

            elif not ciclo["sim_entrada"]:
                ciclo["resumen"] = "Sin entrada — condiciones no alcanzadas en este ciclo"
            else:
                ciclo["resumen"] = "Sin resolucion detectada"

            ciclo["status"] = "completado"
            log.info(f"[TEST] Ciclo {ciclo_num} completado: {ciclo['resumen']}")
            time.sleep(3)   # pausa breve entre ciclos

        # ── Resumen final ────────────────────────────────────────────────────
        total_pnl = sum(c.get("pnl_simulado", 0.0) for c in _test_estado["ciclos"])
        entradas  = sum(1 for c in _test_estado["ciclos"] if c.get("sim_entrada"))
        _test_estado["status"]               = "done"
        _test_estado["finished_at"]          = datetime.now().isoformat()
        _test_estado["pnl_total_simulado"]   = round(total_pnl, 4)
        _test_estado["resumen_final"]        = (
            f"{NUM_CICLOS_TEST} ciclos completados | {entradas} entradas simuladas | "
            f"PnL simulado total: ${total_pnl:+.2f} | Orden fija: ${USD_POR_LADO:.2f}/lado"
        )
        log.info(f"[TEST] Prueba 3 ciclos OK. {_test_estado['resumen_final']}")

    except Exception as e:
        _test_estado["status"] = "error"
        _test_estado["error"]  = str(e)
        import traceback as tb
        _test_estado["traceback"] = tb.format_exc()
        log.error(f"[TEST] Error fatal en prueba: {e}")


# ─── REFRESCO DE SALDO REAL ───────────────────────────────────────────────────

def _refrescar_balance_real():
    """Obtiene el saldo USDC real del CLOB y actualiza estado['capital']."""
    global _balance_real, _ts_ultimo_balance
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        ba  = clob.get_balance_allowance(params)
        bal = float(ba.get("balance", 0)) / 1_000_000
        if bal > 0:
            _balance_real         = bal
            _ts_ultimo_balance    = time.time()
            estado["capital"]     = bal
            if estado["capital_base"] == 0.0:
                estado["capital_base"] = bal   # fijar base del ROI en primer fetch
            estado["peak_capital"] = max(estado["peak_capital"], bal)
            log_ev(f"Balance real actualizado: ${bal:,.2f} USDC")
    except Exception as e:
        log_ev(f"Advertencia al refrescar balance: {e}")


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

# ─── SALDO EN POLYMARKET ──────────────────────────────────────────────────────

def _get_polymarket_balance() -> dict:
    """
    Consulta el saldo USDC via la API del CLOB de Polymarket.
    El USDC no esta en el wallet directamente sino en el contrato del exchange.
    El endpoint /balance-allowance es la fuente correcta.
    Como fallback, tambien revisa ambos contratos USDC en Polygon.
    """
    import requests as req
    result = {"address": PROXY_ADDRESS or "no configurado"}

    # ── 1. Saldo via API CLOB (fuente correcta — USDC en el exchange) ─────────
    if clob:
        try:
            # py-clob-client 0.19: get_balance_allowance con AssetType.COLLATERAL
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            ba = clob.get_balance_allowance(params)
            # ba es un dict con "balance" y "allowance" en unidades mínimas (6 decimales)
            bal = float(ba.get("balance", 0)) / 1_000_000
            result["usdc_label"]   = f"${bal:,.2f} USDC"
            result["usdc_clob"]    = round(bal, 4)
            result["allowance"]    = round(float(ba.get("allowance", 0)) / 1_000_000, 4)
            result["ok"] = True
        except Exception as e:
            result["clob_balance_error"] = str(e)

        try:
            open_orders = clob.get_orders()
            result["open_orders"] = len(open_orders) if open_orders else 0
            result["clob_ok"] = True
        except Exception as e:
            result["clob_ok"] = False
            result["clob_msg"] = str(e)

    # ── 2. Fallback: USDC en wallet Polygon (USDC.e + native USDC) ────────────
    if not result.get("ok") and PROXY_ADDRESS:
        USDC_CONTRACTS = {
            "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            "USDC":   "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        }
        RPC_URLS = ["https://polygon-rpc.com", "https://rpc.ankr.com/polygon"]
        padded   = PROXY_ADDRESS.lower().replace("0x", "").zfill(64)
        total    = 0.0
        for name, contract in USDC_CONTRACTS.items():
            call_data = f"0x70a08231{padded}"
            payload   = {"jsonrpc": "2.0", "method": "eth_call",
                         "params": [{"to": contract, "data": call_data}, "latest"], "id": 1}
            for rpc in RPC_URLS:
                try:
                    r = req.post(rpc, json=payload, timeout=8)
                    hex_val = r.json().get("result", "0x0")
                    bal = int(hex_val, 16) / 1e6
                    result[f"wallet_{name}"] = round(bal, 4)
                    total += bal
                    break
                except Exception:
                    pass
        if total > 0:
            result["usdc_label"] = f"${total:,.2f} USDC (wallet)"
            result["ok"] = True

    return result


if __name__ == "__main__":
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import csv as csv_module
    import io

    if not POLYMARKET_KEY or not PROXY_ADDRESS:
        log.error("ERROR FATAL: Configura POLYMARKET_KEY y PROXY_ADDRESS como variables de entorno.")
        sys.exit(1)

    PORT           = int(os.environ.get("PORT", 8080))
    DASHBOARD_FILE = os.path.join(os.path.dirname(__file__), "templates", "dashboard.html")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # silenciar logs HTTP

        def do_GET(self):
            try:
                if self.path in ("/", "/index.html"):
                    self._serve_dashboard()
                elif self.path == "/api/status":
                    self._serve_status()
                elif self.path == "/api/trades":
                    self._serve_trades()
                elif self.path == "/api/csv":
                    self._serve_csv()
                elif self.path == "/api/balance":
                    self._serve_balance()
                elif self.path == "/api/test" or self.path == "/api/test/status":
                    self._serve_test()
                else:
                    self._send(404, "text/plain", b"Not found")
            except Exception as e:
                self._send(500, "text/plain", str(e).encode())

        def do_POST(self):
            global bot_activo
            if self.path == "/api/start":
                bot_activo = True
                _ts_ultimo_balance = 0  # forzar refresco inmediato de balance
                log_ev("Bot ACTIVADO desde el dashboard.")
                guardar_estado()
                self._send(200, "application/json", b'{"ok":true}')
            elif self.path == "/api/stop":
                bot_activo = False
                log_ev("Bot PAUSADO desde el dashboard.")
                try:
                    clob.cancel_all()
                    log_ev("Ordenes canceladas al pausar.")
                except Exception:
                    pass
                guardar_estado()
                self._send(200, "application/json", b'{"ok":true}')
            else:
                self._send(404, "text/plain", b"Not found")

        def _serve_dashboard(self):
            if os.path.isfile(DASHBOARD_FILE):
                with open(DASHBOARD_FILE, "rb") as f:
                    body = f.read()
                self._send(200, "text/html; charset=utf-8", body)
            else:
                # Fallback minimal — healthcheck de Railway siempre pasa
                self._send(200, "text/html; charset=utf-8",
                           b"<html><body><h2>HEDGE BOT - OK</h2></body></html>")

        def _serve_status(self):
            try:
                if os.path.isfile(STATE_FILE):
                    with open(STATE_FILE) as f:
                        data = json.load(f)
                else:
                    data = {
                        "capital": CAPITAL_INICIAL, "capital_inicial": CAPITAL_INICIAL,
                        "pnl_total": 0, "roi": 0, "win_rate": 0, "wins": 0, "losses": 0,
                        "max_drawdown": 0, "ciclos": 0, "posicion": {"activa": False},
                        "eventos": [], "trades": [], "ts": datetime.now().isoformat(),
                    }
                if "capital_inicial" not in data:
                    data["capital_inicial"] = CAPITAL_INICIAL
                self._send(200, "application/json", json.dumps(data).encode())
            except Exception as e:
                self._send(500, "application/json", json.dumps({"error": str(e)}).encode())

        def _serve_trades(self):
            try:
                if os.path.isfile(LOG_FILE):
                    with open(LOG_FILE) as f:
                        data = json.load(f)
                    trades = data.get("trades", [])
                else:
                    trades = []
                self._send(200, "application/json", json.dumps(trades).encode())
            except Exception:
                self._send(500, "application/json", b"[]")

        def _serve_csv(self):
            try:
                if os.path.isfile(LOG_FILE):
                    with open(LOG_FILE) as f:
                        data = json.load(f)
                    trades = data.get("trades", [])
                else:
                    trades = []

                if not trades:
                    self._send(200, "text/csv", b"sin trades")
                    return

                buf    = io.StringIO()
                writer = csv_module.DictWriter(buf, fieldnames=trades[0].keys())
                writer.writeheader()
                writer.writerows(trades)
                body = buf.getvalue().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Disposition", "attachment; filename=trades.csv")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                self._send(500, "text/plain", str(e).encode())

        def _serve_balance(self):
            try:
                data = _get_polymarket_balance()
                self._send(200, "application/json", json.dumps(data).encode())
            except Exception as e:
                self._send(500, "application/json", json.dumps({"error": str(e)}).encode())

        def _serve_test(self):
            global _test_thread
            try:
                # Si ya hay una prueba corriendo, devolver estado actual (sin lanzar otra)
                if _test_estado["status"] == "running":
                    self._send(200, "application/json", json.dumps(_test_estado, indent=2).encode())
                    return
                # Lanzar nueva prueba en background
                _test_thread = threading.Thread(target=_run_test_3ciclos, daemon=True)
                _test_thread.start()
                # Devolver estado inicial inmediatamente
                self._send(200, "application/json", json.dumps(_test_estado, indent=2).encode())
            except Exception as e:
                self._send(500, "application/json", json.dumps({"ok": False, "error": str(e)}).encode())

        def _send(self, code, ctype, body):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

    def run_server():
        server = HTTPServer(("0.0.0.0", PORT), Handler)
        log.info(f"Dashboard en http://0.0.0.0:{PORT}")
        server.serve_forever()

    # Arrancar servidor web PRIMERO — Railway necesita que / responda en <30s
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    time.sleep(1)
    log.info("Servidor web listo.")

    # Inicializar cliente CLOB (despues del healthcheck)
    init_clob()

    # Cargar balance real
    _refrescar_balance_real()

    log.info("Inicializacion completa — arrancando bot...")

    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        log.info("Detenido por el usuario.")
        log.info("PANICO: Cancelando todas las ordenes activas...")
        try:
            clob.cancel_all()
            log.info("Todas las ordenes canceladas.")
        except Exception as e:
            log.warning(f"Error al cancelar ordenes: {e}. Revisa la web de Polymarket.")
        guardar_estado()
        total = estado["wins"] + estado["losses"]
        wr    = estado["wins"] / total * 100 if total > 0 else 0
        _base = estado["capital_base"] if estado["capital_base"] > 0 else CAPITAL_INICIAL
        roi   = (estado["capital"] - _base) / _base * 100
        print(f"\nCapital: ${estado['capital']:.2f} | ROI: {roi:+.1f}% | W:{estado['wins']} L:{estado['losses']} WR:{wr:.0f}%")
