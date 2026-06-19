#!/usr/bin/env python3
"""
Multi-Roulette Session Bot — 5 Ruletas / Sesiones VIP por horario

Ruletas:
  - Turkish Roulette   — key 224
  - Russian Roulette   — key 221
  - Roulette Macao     — key 206
  - Roulette 2         — key 201
  - Auto Roulette      — key 225

Lógica:
  - Sesiones VIP en horarios fijos (hora Argentina):
      01:00–03:00 | 05:00–07:00 | 09:00–11:00 | 13:00–15:00 | 17:00–19:00 | 21:00–23:00
  - Cada sesión usa una ruleta diferente (rotación secuencial, 5 ruletas para 6 sesiones).
  - Fuera de sesión: solo acumula datos, no emite señales.
  - ALERTA DE OPORTUNIDAD: si los últimos 4 giros pertenecen a solo 2 docenas/columnas,
    se envía aviso. En el giro 5 se evalúa si hay señal; si no, se cancela.
  - Apuesta: 0.50 por categoría (total 1.00). Hasta 2 gales x3 → 1.50 c/u (total 3.00) cada uno. 3 intentos en total.
  - Stats del día enviadas a las 00:00 ARG. Stats globales también a las 00:00.
  - Estadísticas históricas ponderadas exponencialmente (más peso a los datos recientes).

MENSAJES:
  - Los mensajes de inicio VIP de sesión y finalización NO se eliminan del chat.
  - Al iniciar, se cargan los últimos 20 resultados de la API (primer mensaje WS)
    antes de pasar a modo real-time. Luego cada spin llega por evento en tiempo real.
"""

import asyncio
import json
import logging
import math
import os
import sqlite3
import threading
import time
import urllib.request
from collections import deque, defaultdict
from typing import Optional, Dict

import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier

import telebot
import websockets
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [MultiRoulette] %(levelname)s %(message)s')
logger = logging.getLogger("MultiRoulette")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN          = "8347707121:AAH1cPEDMLbm-scTJ8mUuufeEhzw3Axv2Lw"
CHAT_ID        = -1003835197023
STATS_THREAD_ID = 40034   # Tema del grupo donde se envían las estadísticas diarias

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

# ─── RULETAS CONFIGURACIÓN ────────────────────────────────────────────────────
ROULETTES = [
    {"key": 224, "name": "TURKISH ROULETTE"},
    {"key": 221, "name": "RUSSIAN ROULETTE"},
    {"key": 206, "name": "ROULETTE MACAO"},
    {"key": 201, "name": "ROULETTE 2"},
    {"key": 225, "name": "AUTO ROULETTE"},
]

# ─── LINKS POR RULETA ─────────────────────────────────────────────────────────
ROULETTE_LINKS = {
    "AUTO ROULETTE":    "https://1win.lat/casino/play/v_pragmatic:1winautoroulette",
    "ROULETTE 2":       "https://1win.lat/casino/play/v_pragmatic:roulette2",
    "ROULETTE MACAO":   "https://1win.lat/casino/play/v_pragmatic:roulettemacao",
    "RUSSIAN ROULETTE": "https://1win.lat/casino/play/v_pragmatic:1winroulette",
    "TURKISH ROULETTE": "https://1win.lat/casino/play/v_pragmatic:1winturkishroulette",
}

def get_roulette_url(name: str) -> Optional[str]:
    """Devuelve el link de la ruleta ignorando emojis y variaciones de nombre."""
    # Buscar coincidencia exacta primero
    if name in ROULETTE_LINKS:
        return ROULETTE_LINKS[name]
    # Buscar por nombre base (sin emoji)
    name_upper = name.upper()
    for key, url in ROULETTE_LINKS.items():
        if key.upper() in name_upper or name_upper in key.upper():
            return url
    return None

def tg_send_with_button(text: str, roulette_name: str) -> Optional[int]:
    """Envía mensaje con botón inline de acceso a la ruleta (sin pie de página del link)."""
    url = get_roulette_url(roulette_name)
    if not url:
        return tg_send(text)
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton("🎰 ACCEDER A LA RULETA", url=url))
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                   parse_mode="HTML", reply_markup=markup,
                   disable_web_page_preview=True)
    return msg.message_id if msg else None

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
WS_URL         = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID      = "ppcjd00000007254"
# Sesiones VIP: (hora_inicio, hora_fin) en hora Argentina
VIP_SESSIONS = [
    (1, 3),
    (5, 7),
    (9, 11),
    (13, 15),
    (17, 19),
    (21, 23),
]
WARMUP_SPINS   = 200
MIN_PROB       = 0.80
TRAIN_INTERVAL = 100

REAL_COLOR_MAP: dict = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO",
}

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_RETRIES = 12
def _tg_call(fn, *a, **kw):
    delay = 2.0
    for attempt in range(1, _TG_RETRIES + 1):
        try: return fn(*a, **kw)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try: wait = int(''.join(filter(str.isdigit, err))) + 1
                except: wait = 30
                time.sleep(wait); continue
            if attempt == _TG_RETRIES: return None
            time.sleep(delay); delay = min(delay * 2, 60)
    return None

def tg_send(text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text, parse_mode="HTML")
    return msg.message_id if msg else None

def tg_send_stats(text: str) -> Optional[int]:
    """Envía al tema (topic) 40034 del grupo — exclusivo para el reporte diario de stats."""
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                   parse_mode="HTML", message_thread_id=STATS_THREAD_ID)
    return msg.message_id if msg else None

def tg_delete(chat_id: int, message_id: int):
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
    except:
        pass

def tg_edit(chat_id: int, message_id: int, text: str):
    """Edita un mensaje existente en Telegram."""
    try:
        _tg_call(bot.edit_message_text, text=text, chat_id=chat_id,
                 message_id=message_id, parse_mode="HTML")
    except:
        pass

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if len(data) < period: return [None] * len(data)
    mult = 2 / (period + 1)
    out = [None] * (period - 1)
    prev = sum(data[:period]) / period
    out.append(prev)
    for v in data[period:]:
        prev = v * mult + prev * (1 - mult)
        out.append(prev)
    return out

def ema_signal(levels: list, mode: str = "moderado") -> bool:
    if len(levels) < 20: return False
    e4, e8, e20 = calc_ema(levels, 4), calc_ema(levels, 8), calc_ema(levels, 20)
    li = len(levels) - 1
    if any(v is None for v in [e4[li], e8[li], e20[li]]): return False
    cur = levels[li]; ce4, ce8, ce20 = e4[li], e8[li], e20[li]
    pe4  = e4[li-1]  if li > 0 and e4[li-1]  is not None else ce4
    pe8  = e8[li-1]  if li > 0 and e8[li-1]  is not None else ce8
    pe20 = e20[li-1] if li > 0 and e20[li-1] is not None else ce20
    if mode == "tendencia":
        return (pe4 <= pe20 and ce4 > ce20) or (cur > ce4 and cur > ce8 and cur > ce20)
    else:
        v_pattern = False
        if len(levels) >= 3:
            a, b, c = levels[-3], levels[-2], levels[-1]
            v_pattern = (b < a) and (b < c) and (c > a)
        return (pe4 <= pe8 and ce4 > ce8) or (pe8 <= pe20 and ce8 > ce20) or (cur > ce4 and cur > ce8) or v_pattern

# ─── MARKOV ───────────────────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window; self.order = order
        self.transition_counts: dict = {}

    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1: return
        for i in range(len(recent) - self.order):
            state = tuple(recent[i:i + self.order])
            nxt = recent[i + self.order]
            self.transition_counts[state][nxt] += 1

    def predict(self, sequence: list) -> Optional[dict]:
        if len(sequence) < self.order: return None
        state = tuple(sequence[-self.order:])
        counts = dict(self.transition_counts.get(state, {}))
        total = sum(counts.values())
        if total < 10: return None
        alpha = 2.0; vocab_size = 3
        probs = {k: (v + alpha) / (total + alpha * vocab_size) for k, v in counts.items()}
        for c in [1, 2, 3]:
            if c not in probs: probs[c] = alpha / (total + alpha * vocab_size)
        return probs

# ─── ENSEMBLE ML ──────────────────────────────────────────────────────────────
class OnlineEnsemblePredictor:
    WINDOW = 5; CLASSES = [1, 2, 3]

    def __init__(self):
        self.mnb = MultinomialNB(alpha=2.0, class_prior=[0.333, 0.333, 0.333])
        self.sgd = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.005,
                                 penalty='l2', alpha=0.01, epsilon=0.2)
        self.trained = False; self.sample_count = 0

    def _extract_features(self, hist_d, hist_c, pf_pair_d, ph_pair_d, pf_pair_c, ph_pair_c) -> Optional[list]:
        if len(hist_d) < self.WINDOW or len(hist_c) < self.WINDOW: return None
        features = []
        for i in range(1, self.WINDOW + 1):
            d = hist_d[-i]; c = hist_c[-i]
            vec = [0] * 9; vec[(d - 1) * 3 + (c - 1)] = 1
            features.extend(vec)
        for pair in (pf_pair_d, ph_pair_d, pf_pair_c, ph_pair_c):
            vec = [0, 0, 0]
            for x in pair: vec[x - 1] = 1
            features.extend(vec)
        return features

    def partial_train(self, hist_d, hist_c, target, pf_d, ph_d, pf_c, ph_c):
        feats = self._extract_features(hist_d[:-1], hist_c[:-1], pf_d, ph_d, pf_c, ph_c)
        if feats is None: return
        X = np.array(feats).reshape(1, -1); y = np.array([target])
        if not self.trained:
            self.mnb.partial_fit(X, y, classes=self.CLASSES)
            self.sgd.partial_fit(X, y, classes=self.CLASSES)
            self.trained = True
        else:
            self.mnb.partial_fit(X, y); self.sgd.partial_fit(X, y)
        self.sample_count += 1

    def predict(self, hist_d, hist_c, pf_d, ph_d, pf_c, ph_c) -> Optional[dict]:
        if not self.trained: return None
        feats = self._extract_features(hist_d, hist_c, pf_d, ph_d, pf_c, ph_c)
        if feats is None: return None
        X = np.array(feats).reshape(1, -1)
        try:
            nb_p = self.mnb.predict_proba(X)[0]
            sg_p = self.sgd.predict_proba(X)[0]
            final = 0.5 * nb_p + 0.5 * sg_p
            return {c + 1: float(p) for c, p in enumerate(final)}
        except: return None

# ─── STATS GLOBAL UNIFICADA ───────────────────────────────────────────────────
class GlobalStats:
    """
    Estadísticas en dos niveles:
      - Diarias: se reinician a las 00:00 ARG.
      - Globales: acumulan desde el inicio del bot, nunca se resetean.
    El reporte se envía a las 00:00 ARG (stats del día + stats globales).
    """

    def __init__(self):
        # Estadísticas del día (se reinician a las 00:00 ARG)
        self.day_wins = 0; self.day_zeros = 0; self.day_losses = 0

        # Estadísticas globales (nunca se reinician)
        self.total_wins = 0; self.total_zeros = 0; self.total_losses = 0
        self.consecutive = 0   # consecutivas globales

    def record(self, result_type: str, attempt: int, number: int,
               val, type_str: str, roulette_name: str):
        if result_type == 'WIN':
            self.day_wins   += 1
            self.total_wins += 1
            self.consecutive += 1
        elif result_type == 'LOSS':
            self.day_losses   += 1
            self.total_losses += 1
            self.consecutive = 0
        elif result_type == 'EMPATE':
            self.day_zeros   += 1
            self.total_zeros += 1

    def _reset_day(self):
        self.day_wins = 0; self.day_zeros = 0; self.day_losses = 0

    def get_midnight_text(self) -> str:
        import datetime
        ARG_UTC_OFFSET = -3
        yesterday = (datetime.datetime.utcnow() + datetime.timedelta(hours=ARG_UTC_OFFSET)
                     - datetime.timedelta(days=1))
        day_label = yesterday.strftime("%-d de %B de %Y").capitalize()
        # Convertir nombre del mes al español
        _meses = {
            "January": "enero", "February": "febrero", "March": "marzo",
            "April": "abril", "May": "mayo", "June": "junio",
            "July": "julio", "August": "agosto", "September": "septiembre",
            "October": "octubre", "November": "noviembre", "December": "diciembre",
        }
        for en, es in _meses.items():
            day_label = day_label.replace(en, es)

        day_total = self.day_wins + self.day_zeros + self.day_losses
        gl_total  = self.total_wins + self.total_zeros + self.total_losses
        gl_eff    = ((self.total_wins + self.total_zeros) / gl_total * 100) if gl_total > 0 else 0.0

        text  = f"🤑 Resultados del {day_label} 🤑\n"
        text += f"📆 Total de señales: {day_total}\n"
        text += f"✅ Señales Ganadas: {self.day_wins}\n"
        text += f"🟠 Señales Empate: {self.day_zeros}\n"
        text += f"❌ Señales Pérdidas: {self.day_losses}\n\n"

        text += "📆 MARCADOR GLOBAL\n\n"
        text += f"🔵 PLACAR = ✅{self.total_wins} | 🟠{self.total_zeros} | 🚫{self.total_losses}\n"
        text += f"🔴 Consecutivas = {self.consecutive}\n"
        text += f"🟣 Assertividade = {gl_eff:.2f}%\n"
        text += f"🟡 Total señales: {gl_total}"
        return text

# Instancia global única de stats
GLOBAL_STATS = GlobalStats()

# ─── ENGINE POR RULETA ────────────────────────────────────────────────────────
class RouletteEngine:
    """Estado ML/PF/PH independiente por ruleta. Sin gestión de niveles (solo nivel 1)."""

    def __init__(self, ws_key: int, name: str):
        self.ws_key = ws_key
        self.name = name
        self.db_path = f"roulette_{ws_key}.db"

        # Estado analítico
        self.spin_history: list = []
        self.dozen_seq: list = []; self.column_seq: list = []
        self.d_levels: dict = {1: [], 2: [], 3: []}
        self.c_levels: dict = {1: [], 2: [], 3: []}
        self.markov_d = SmoothedMarkovPredictor()
        self.markov_c = SmoothedMarkovPredictor()
        self.ensemble_d = OnlineEnsemblePredictor()
        self.ensemble_c = OnlineEnsemblePredictor()
        self.after_number_dozen: dict = defaultdict(lambda: defaultdict(int))
        self.after_number_column: dict = defaultdict(lambda: defaultdict(int))

        # Estado de señal
        self.signal_active = False
        self.active_type = None
        self.active_pair: tuple = ()
        self.active_missing = ""
        self.oportunidad = 1         # 1 = entrada base, 2 = gale x3
        self.active_signal_msg_id = None

        # Estado de alerta de oportunidad
        self.oportunidad_alerta = False         # True cuando se envió alerta de oportunidad
        self.oportunidad_spin   = 0             # giros transcurridos desde la alerta
        self.oportunidad_alert_msg_id: Optional[int] = None  # msg_id para poder borrarla
        self.spins_since_train = 0
        self.last_game_id = None
        self.ws_count = 0
        self.warmup_done = False

        self._db = self._get_db()
        live = self._load_live_history()
        self.ws_count = live
        self.warmup_done = live >= WARMUP_SPINS
        logger.info(f"[{name}] Pre-cargados: {live} giros | Warmup: {'✅' if self.warmup_done else '⏳'}")

    # ── DB ────────────────────────────────────────────────────────────────────
    def _get_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("""CREATE TABLE IF NOT EXISTS live_spins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number INTEGER NOT NULL,
            ts INTEGER NOT NULL
        )""")
        conn.commit()
        return conn

    def _persist(self, number: int):
        try:
            self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)", (number, int(time.time())))
            self._db.commit()
        except: pass

    def _load_live_history(self) -> int:
        try: rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except: return 0
        for (n,) in rows: self._update_state(n, persist=False, train_model=False)
        if rows: self._train_models()
        return len(rows)

    def _train_models(self):
        self.markov_d.update(self.dozen_seq)
        self.markov_c.update(self.column_seq)

    # ── Estado ────────────────────────────────────────────────────────────────
    def _update_state(self, number: int, persist=True, train_model=True):
        color = REAL_COLOR_MAP.get(number, "VERDE")
        d = get_dozen(number); c = get_column(number)
        if number != 0 and self.spin_history:
            prev = self.spin_history[-1]["number"]
            if prev != 0:
                self.after_number_dozen[prev][d] += 1
                self.after_number_column[prev][c] += 1
        self.spin_history.append({"number": number, "color": color})
        if d != 0:
            self.dozen_seq.append(d)
            for dd in (1, 2, 3):
                prev = self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev + (1 if d == dd else -1))
        if c != 0:
            self.column_seq.append(c)
            for cc in (1, 2, 3):
                prev = self.c_levels[cc][-1] if self.c_levels[cc] else 0
                self.c_levels[cc].append(prev + (1 if c == cc else -1))
        if train_model and d != 0 and c != 0 and len(self.dozen_seq) > 5:
            pf_d, ph_d = self._get_pf("DOCENA"), self._get_ph("DOCENA")
            pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
            if pf_d and ph_d and pf_c and ph_c:
                self.ensemble_d.partial_train(self.dozen_seq, self.column_seq, d,
                                              pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
                self.ensemble_c.partial_train(self.dozen_seq, self.column_seq, c,
                                              pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
            self.spins_since_train += 1
            if self.spins_since_train >= TRAIN_INTERVAL:
                self._train_models(); self.spins_since_train = 0
        if persist: self._persist(number)

    def _get_pf(self, cat_type: str) -> Optional[Dict]:
        if len(self.spin_history) < 5: return None
        counts = {1: 0, 2: 0, 3: 0}
        for s in self.spin_history[-5:]:
            n = s["number"]
            if n != 0:
                val = get_dozen(n) if cat_type == "DOCENA" else get_column(n)
                counts[val] += 1
        active = [k for k, v in counts.items() if v > 0]
        if len(active) != 2: return None
        missing = list({1, 2, 3} - set(active))[0]
        return {"pair": tuple(sorted(active)), "missing": missing, "prob": sum(counts[a] for a in active) / 5.0}

    def _get_ph(self, cat_type: str) -> Optional[Dict]:
        """Estadísticas históricas ponderadas exponencialmente.
        Los eventos más recientes tienen mayor peso (decay=0.97 por posición).
        """
        if not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None

        # ── Reconstruir conteos ponderados desde spin_history ─────────────────
        # Recorremos la historia buscando ocurrencias de last_num y ponderando
        # el siguiente número según su antigüedad (posición más reciente = peso 1.0)
        DECAY = 0.97
        weighted: dict = {1: 0.0, 2: 0.0, 3: 0.0}
        total_weight = 0.0
        history = self.spin_history

        # Encontrar pares (last_num → siguiente) en orden cronológico inverso
        pairs_found = 0
        for i in range(len(history) - 2, -1, -1):
            if history[i]["number"] == last_num and history[i + 1]["number"] != 0:
                nxt = history[i + 1]["number"]
                val = (get_dozen(nxt) if cat_type == "DOCENA" else get_column(nxt))
                if val in weighted:
                    w = DECAY ** pairs_found
                    weighted[val] += w
                    total_weight  += w
                    pairs_found   += 1

        if total_weight < 5.0 or pairs_found < 5:
            return None

        # Normalizar
        probs = {k: v / total_weight for k, v in weighted.items()}
        sc = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        if len(sc) < 2: return None
        missing = list({1, 2, 3} - {sc[0][0], sc[1][0]})[0]
        return {
            "pair": tuple(sorted([sc[0][0], sc[1][0]])),
            "missing": missing,
            "prob": sc[0][1] + sc[1][1],
        }

    def _predict_pair_ml(self, cat_type: str, missing_num: int) -> float:
        mk   = self.markov_d if cat_type == "DOCENA" else self.markov_c
        hist = self.dozen_seq if cat_type == "DOCENA" else self.column_seq
        levels = (self.d_levels if cat_type == "DOCENA" else self.c_levels).get(missing_num, [])
        mk_pred = mk.predict(hist)
        m_p_miss = mk_pred.get(missing_num, 1/3) if mk_pred else 1/3
        pf_d, ph_d = self._get_pf("DOCENA"), self._get_ph("DOCENA")
        pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
        ens_p_miss = 1/3
        if pf_d and ph_d and pf_c and ph_c:
            ens = self.ensemble_d.predict(hist, self.column_seq, pf_d["pair"], ph_d["pair"],
                                          pf_c["pair"], ph_c["pair"]) \
                  if cat_type == "DOCENA" else \
                  self.ensemble_c.predict(self.dozen_seq, hist, pf_d["pair"], ph_d["pair"],
                                          pf_c["pair"], ph_c["pair"])
            if ens: ens_p_miss = ens.get(missing_num, 1/3)
        ml_miss = 0.4 * m_p_miss + 0.6 * ens_p_miss
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ml_miss *= 0.85
            elif ema_signal(levels, "moderado"): ml_miss *= 0.92
        return 1.0 - ml_miss

    def detect_signal(self) -> Optional[dict]:
        pf_d = self._get_pf("DOCENA"); pf_c = self._get_pf("COLUMNA")
        if not pf_d and not pf_c: return None
        ph_d = self._get_ph("DOCENA"); ph_c = self._get_ph("COLUMNA")
        candidates = []
        if pf_d and ph_d and set(pf_d["pair"]) == set(ph_d["pair"]):
            base = 0.65 * pf_d["prob"] + 0.35 * ph_d["prob"]
            ml   = self._predict_pair_ml("DOCENA", pf_d["missing"])
            prob = 0.5 * base + 0.5 * ml
            if prob >= MIN_PROB:
                candidates.append({"type":"DOCENA",
                                    "pair":tuple(f"D{x}" for x in sorted(pf_d["pair"])),
                                    "missing":f"D{pf_d['missing']}", "prob":prob})
        if pf_c and ph_c and set(pf_c["pair"]) == set(ph_c["pair"]):
            base = 0.65 * pf_c["prob"] + 0.35 * ph_c["prob"]
            ml   = self._predict_pair_ml("COLUMNA", pf_c["missing"])
            prob = 0.5 * base + 0.5 * ml
            if prob >= MIN_PROB:
                candidates.append({"type":"COLUMNA",
                                    "pair":tuple(f"C{x}" for x in sorted(pf_c["pair"])),
                                    "missing":f"C{pf_c['missing']}", "prob":prob})
        return max(candidates, key=lambda x: x["prob"]) if candidates else None

    # ── Mensaje de señal ──────────────────────────────────────────────────────
    def _build_signal_text(self) -> str:
        nums = sorted([p[1:] for p in self.active_pair])
        pair_disp = f"{nums[0]} y {nums[1]}"
        type_str  = ("docenas" if self.active_type == "DOCENA" else "columnas")
        gale_num  = self.oportunidad - 1   # 0 = entrada base, 1 = gale 1, 2 = gale 2
        return (
            f"✅✅ ENTRADA CONFIRMADA ✅✅\n\n"
            f"🕹️ {self.name}\n"
            f"🎯 Entrar en las {type_str}: {pair_disp}\n"
            f"⚔️ Cubrir el CERO 🟢\n"
            f"🛟 GALE #{gale_num}"
        )

    def send_signal(self):
        msg_id = tg_send_with_button(self._build_signal_text(), self.name)
        if msg_id:
            self.active_signal_msg_id = msg_id

    def iniciar_senal(self, sig: dict):
        self.signal_active    = True
        self.active_type      = sig["type"]
        self.active_pair      = sig["pair"]
        self.active_missing   = sig["missing"]
        self.oportunidad      = 1
        self.send_signal()

    def resolve(self, number: int) -> bool:
        """Resolver resultado. Retorna True si la señal terminó."""
        d, c     = get_dozen(number), get_column(number)
        type_str = self.active_type
        val_num  = d if type_str == "DOCENA" else c
        intento  = self.oportunidad   # 1, 2 o 3
        gale_num = intento - 1        # 0 = entrada base, 1 = gale 1, 2 = gale 2

        # ── CERO ──────────────────────────────────────────────────────────────
        if number == 0:
            tg_send(f"🟠 SALIO CERO 🔄 GALE #{gale_num}")
            GLOBAL_STATS.record('EMPATE', intento, 0, 0, type_str, self.name)
            self._reset_signal()
            return True

        won = (type_str == "DOCENA"  and d != 0 and f"D{d}" in self.active_pair) or \
              (type_str == "COLUMNA" and c != 0 and f"C{c}" in self.active_pair)

        if won:
            cat_label = f"{'DOCENA' if type_str == 'DOCENA' else 'COLUMNA'} {val_num}"
            GLOBAL_STATS.record('WIN', intento, number, val_num, type_str, self.name)
            tg_send(f"✅ WIN {number} — {cat_label} — 🔄 GALE #{gale_num}")
            self._reset_signal()
            return True

        else:
            if intento < 3:
                # Perdió este intento → borrar señal, reenviar con el siguiente GALE
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None
                self.oportunidad = intento + 1
                self.send_signal()
                return False   # señal sigue activa

            else:
                # Perdió Gale #2 (3er intento) → LOSS
                cat_label = f"{'DOCENA' if type_str == 'DOCENA' else 'COLUMNA'} {val_num}"
                GLOBAL_STATS.record('LOSS', 3, number, val_num, type_str, self.name)
                tg_send(f"❌ LOSS {number} — {cat_label} — 🔄 GALE #{gale_num}")
                self._reset_signal()
                return True

    def _check_oportunidad(self) -> bool:
        """
        Devuelve True si los últimos 4 giros (sin cero) pertenecen a exactamente
        2 docenas O exactamente 2 columnas. Si ya hay alerta activa, no re-emite.
        """
        if self.signal_active or self.oportunidad_alerta:
            return False
        # Tomar últimos 4 resultados sin cero
        no_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(no_zero) < 4:
            return False
        last4 = no_zero[-4:]
        dozens  = {get_dozen(n) for n in last4}
        columns = {get_column(n) for n in last4}
        return len(dozens) == 2 or len(columns) == 2

    def send_oportunidad_alert(self):
        """Envía el mensaje de alerta de oportunidad y guarda su ID para poder borrarlo."""
        msg_id = tg_send(
            "🚨 OPORTUNIDAD EN EVALUACIÓN 🚨\n"
            "Preparado jugador para tomar la oportunidad..."
        )
        self.oportunidad_alerta       = True
        self.oportunidad_spin         = 0
        self.oportunidad_alert_msg_id = msg_id

    def _reset_signal(self):
        self.signal_active = False
        self.active_pair = ()
        self.active_type = None
        self.oportunidad = 1
        self.active_signal_msg_id = None
        self.oportunidad_alerta = False
        self.oportunidad_spin   = 0
        self.oportunidad_alert_msg_id = None

    def feed_number(self, number: int, active: bool = False):
        """Alimentar número al estado del engine. Persiste en DB y loguea en consola."""
        color  = REAL_COLOR_MAP.get(number, "VERDE")
        d      = get_dozen(number)
        c      = get_column(number)
        tag    = "🟢 ACTIVA" if active else "⚫ pasiva"
        spin_n = len(self.spin_history) + 1  # antes del update

        self._update_state(number)           # persiste en DB aquí

        if not self.warmup_done:
            self.ws_count += 1
            warmup_tag = f"⏳ warmup {self.ws_count}/{WARMUP_SPINS}"
            if self.ws_count >= WARMUP_SPINS:
                self.warmup_done = True
                warmup_tag = "✅ WARMUP listo"
        else:
            warmup_tag = "✔"

        logger.info(
            f"[{self.name}] 🎰 #{spin_n:>4} | {number:>2} {color:<5} D{d} C{c} "
            f"| {tag} | {warmup_tag} | 💾 guardado"
        )


# ─── GESTOR DE SESIONES ───────────────────────────────────────────────────────
class SessionManager:
    """
    Sesiones VIP en horarios fijos (hora Argentina):
      01:00–03:00 | 05:00–07:00 | 09:00–11:00 | 13:00–15:00 | 17:00–19:00 | 21:00–23:00

    - Cada sesión usa una ruleta diferente (rotación secuencial por sesión).
      Son 6 sesiones y 5 ruletas → la ruleta se repite cada 5 sesiones (módulo 5).
    - Fuera de sesión: solo acumula datos, no emite señales ni alertas.
    - ALERTA DE OPORTUNIDAD: si últimos 4 giros son de 2 docenas/columnas, avisa.
      En el giro 5 evalúa si hay señal; si no, cancela la alerta.
    - Aviso PRE-SESIÓN: 15 minutos antes de cada sesión, se envía un mensaje
      anunciando la ruleta que se usará.
    - Aviso POST-SESIÓN: 10 minutos después de terminar cada sesión, se envía
      un mensaje con el cronograma completo de sesiones del día.
    """

    def __init__(self):
        self.engines: list[RouletteEngine] = [
            RouletteEngine(r["key"], r["name"]) for r in ROULETTES
        ]
        self.current_idx            = 0   # índice de sesión actual (rota por sesión)
        self.session_active         = False
        self.signal_sent_this_session = False

        # ── IDs para borrar mensajes anteriores ───────────────────────────────
        self.prev_start_msg_id: Optional[int] = None
        self.prev_end_msg_id:   Optional[int] = None

        # ── Control de avisos pre/post sesión (evitar reenvíos duplicados) ────
        self._preavviso_sent_for: Optional[tuple] = None   # (idx_sesión, fecha) ya avisada
        self._postaviso_sent_for: Optional[tuple] = None   # (idx_sesión, fecha) ya avisada

        logger.info("[SessionManager] Iniciado — sesiones VIP por horario.")

    # ── Hora Argentina ────────────────────────────────────────────────────────
    @staticmethod
    def _now_arg():
        import datetime
        return datetime.datetime.utcnow() + datetime.timedelta(hours=-3)

    # ── ¿Estamos dentro de una sesión VIP? ───────────────────────────────────
    @staticmethod
    def _in_vip_session() -> bool:
        now = SessionManager._now_arg()
        h = now.hour
        for (start_h, end_h) in VIP_SESSIONS:
            if start_h <= h < end_h:
                return True
        return False

    # ── Próxima sesión (índice de VIP_SESSIONS y datetime de inicio) ─────────
    @staticmethod
    def _next_session_start():
        import datetime
        now = SessionManager._now_arg()
        today = now.date()
        candidates = []
        for i, (start_h, _) in enumerate(VIP_SESSIONS):
            start_dt = datetime.datetime(today.year, today.month, today.day, start_h, 0, 0)
            if start_dt <= now:
                start_dt += datetime.timedelta(days=1)
            candidates.append((i, start_dt))
        return min(candidates, key=lambda x: x[1])

    # ── Enviar mensaje de inicio de sesión ────────────────────────────────────
    def _send_start_message(self):
        engine = self.engines[self.current_idx]
        # Los mensajes de inicio y fin VIP NO se eliminan del chat (quedan como registro)
        msg_id = tg_send("⭐️ ¡Sesión VIP iniciada! ⭐️")
        self.prev_start_msg_id = msg_id
        logger.info(f"[SessionManager] 🟢 Sesión VIP iniciada: {engine.name}")

    # ── Enviar mensaje de fin de sesión ───────────────────────────────────────
    def _send_end_message(self):
        engine = self.engines[self.current_idx]
        msg_id = tg_send("⭐️ ¡Sesión VIP finalizada! ⭐️")
        self.prev_end_msg_id = msg_id
        logger.info(f"[SessionManager] 🔴 Sesión VIP finalizada: {engine.name}")
        # Avanzar índice para la próxima sesión
        self.current_idx = (self.current_idx + 1) % len(self.engines)

    # ── Enviar aviso 15 min antes de iniciar sesión ───────────────────────────
    def _send_pre_session_message(self, roulette_name: str):
        """Envía aviso 15 min antes con la ruleta de la próxima sesión (current_idx) y botón de acceso."""
        text = (
            "EN BREVE COMENZAMOS NUESTRA\n"
            "SESIÓN GRATUITA EN 15 MINUTOS 💪\n\n"
            "🟥 Antes de comenzar 🟥\n\n"
            "Para aprovechar mejor la sesión, recomendamos un capital inicial de(5 USD)\n\n"
            "Este monto te permite operar con mayor control, mejor gestión\n"
            "y aprovechar las oportunidades con más tranquilidad. 📈\n\n"
            f"⚪ USAREMOS {roulette_name} ⚪"
        )
        tg_send_with_button(text, roulette_name)
        logger.info(f"[SessionManager] 🔔 Aviso pre-sesión enviado (15 min antes) — {roulette_name}")

    # ── Enviar aviso 10 min después de finalizar sesión ──────────────────────
    def _send_post_session_message(self):
        text = (
            "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥 \n\n"
            "▶️ SESIONES EN EL DIA\n\n"
            "📍S1 — 1:00 HASTA 3:00\n"
            "📍S2 — 5:00 HASTA 7:00\n"
            "📍S3 — 9:00 HASTA 11:00\n"
            "📍S4 — 13:00 HASTA 15:00\n"
            "📍S5 — 17:00 HASTA 19:00 \n"
            "📍S6 — 21:00 HASTA 23:00\n\n"
            "⚠️IMPORTANTE: Estos horarios son basados en el horario de Argentina 🇦🇷\n\n"
            "Consulta tu horario en tu país…\n"
            "DUDAS? Habla conmigo @PinguiRoulette 🚀\n\n"
            "🔞 Juega con Responsabilidad"
        )
        tg_send(text)
        logger.info("[SessionManager] 📋 Aviso post-sesión enviado (10 min después)")

    # ── Watchdog: vigila inicio/fin de sesión VIP + avisos pre/post ──────────
    async def session_watchdog(self):
        import datetime
        was_in_session = False
        last_session_end: Optional[datetime.datetime] = None  # datetime en que terminó la última sesión

        while True:
            await asyncio.sleep(10)
            now = self._now_arg()
            in_session = self._in_vip_session()

            # ── Aviso PRE-SESIÓN: 15 min antes de la próxima sesión ──────────
            if not in_session:
                next_idx, next_start = self._next_session_start()
                minutes_to_start = (next_start - now).total_seconds() / 60.0
                key = (next_idx, next_start.date(), next_start.hour)
                if 0 <= minutes_to_start <= 15 and self._preavviso_sent_for != key:
                    # La ruleta de la próxima sesión es siempre self.current_idx
                    # (current_idx avanza al TERMINAR cada sesión, no al comenzar)
                    roulette_name = self.engines[self.current_idx].name
                    self._send_pre_session_message(roulette_name)
                    self._preavviso_sent_for = key

            # ── Aviso POST-SESIÓN: 10 min después de terminar ────────────────
            if last_session_end is not None:
                minutes_since_end = (now - last_session_end).total_seconds() / 60.0
                key = (last_session_end.date(), last_session_end.hour, last_session_end.minute)
                if minutes_since_end >= 10 and self._postaviso_sent_for != key:
                    self._send_post_session_message()
                    self._postaviso_sent_for = key

            if in_session and not was_in_session:
                # Entrar a sesión VIP
                self.session_active = True
                self.signal_sent_this_session = False
                engine = self.engines[self.current_idx]
                engine._reset_signal()
                self._send_start_message()
                was_in_session = True

            elif not in_session and was_in_session:
                # Salir de sesión VIP
                self.session_active = False
                engine = self.engines[self.current_idx]
                # Borrar alerta de oportunidad si sigue publicada en el chat
                if engine.oportunidad_alerta and engine.oportunidad_alert_msg_id:
                    tg_delete(CHAT_ID, engine.oportunidad_alert_msg_id)
                    engine.oportunidad_alert_msg_id = None
                engine._reset_signal()
                self._send_end_message()
                last_session_end = now
                was_in_session = False

    # ── Tick de sesión activa ─────────────────────────────────────────────────
    def tick_active(self, engine: RouletteEngine, number: int):
        engine.feed_number(number, active=True)

        if not self.session_active:
            return

        # ── Señal activa → resolver ───────────────────────────────────────────
        if engine.signal_active:
            done = engine.resolve(number)
            if done:
                self.signal_sent_this_session = True
            return

        # ── Sin límite de señales mientras la sesión esté activa ──────────────
        if not engine.warmup_done:
            return

        # ── Alerta de oportunidad activa → evaluar en cada giro ──────────────
        if engine.oportunidad_alerta:
            engine.oportunidad_spin += 1
            sig = engine.detect_signal()
            if sig:
                # Hay señal → borrar alerta y confirmar entrada
                if engine.oportunidad_alert_msg_id:
                    tg_delete(CHAT_ID, engine.oportunidad_alert_msg_id)
                    engine.oportunidad_alert_msg_id = None
                engine.oportunidad_alerta = False
                engine.oportunidad_spin   = 0
                logger.info(f"[SessionManager] 🎯 Señal detectada tras alerta: {sig}")
                engine.iniciar_senal(sig)
            else:
                # Sin señal → borrar mensaje de alerta
                if engine.oportunidad_alert_msg_id:
                    tg_delete(CHAT_ID, engine.oportunidad_alert_msg_id)
                    engine.oportunidad_alert_msg_id = None
                # ¿La tendencia PF sigue (últimos 4 en 2 docenas/columnas)?
                engine.oportunidad_alerta = False   # reset temporal para que _check_oportunidad funcione
                if engine._check_oportunidad():
                    # Tendencia continúa → reenviar alerta
                    logger.info(f"[SessionManager] 🔄 Tendencia continúa — reenviando alerta de oportunidad.")
                    engine.send_oportunidad_alert()
                else:
                    # Tendencia rota → cancelar definitivamente
                    engine.oportunidad_spin = 0
                    logger.info(f"[SessionManager] ⚠️ Tendencia rota — oportunidad descartada.")
            return

        # ── Chequear oportunidad ──────────────────────────────────────────────
        if engine._check_oportunidad():
            engine.send_oportunidad_alert()
            return

        # ── Detección normal de señal ─────────────────────────────────────────
        sig = engine.detect_signal()
        if sig:
            logger.info(f"[SessionManager] 🎯 Señal detectada en {engine.name}: {sig}")
            engine.iniciar_senal(sig)
            self.signal_sent_this_session = True

    # ── Tick pasivo ───────────────────────────────────────────────────────────
    def tick_passive(self, engine: RouletteEngine, number: int):
        engine.feed_number(number, active=False)

    # ── Entrada de número desde WS ────────────────────────────────────────────
    def on_number(self, ws_key: int, number: int):
        for i, engine in enumerate(self.engines):
            if engine.ws_key != ws_key:
                continue
            if i == self.current_idx:
                self.tick_active(engine, number)
            else:
                self.tick_passive(engine, number)
            break

    # ── Avanzar ruleta manualmente (comando /siguiente) ───────────────────────
    def _advance_session(self):
        engine = self.engines[self.current_idx]
        engine._reset_signal()
        self.current_idx = (self.current_idx + 1) % len(self.engines)
        self.signal_sent_this_session = False
        logger.info(f"[SessionManager] ↪️ Cambio manual → {self.engines[self.current_idx].name}")



# ─── WS READER POR RULETA ─────────────────────────────────────────────────────
async def ws_reader(ws_key: int, session_mgr: SessionManager):
    """
    Lee datos WebSocket para una ruleta con polling activo de 1 segundo.

    Deduplicación:
      - Cada jugada tiene un game_id único proporcionado por el servidor.
      - Se mantiene un set `seen_ids` acotado (máx 200 IDs) + deque para
        limpiar los más antiguos automáticamente y evitar crecimiento infinito.
      - TODAS las rutas de entrada (last20Results y fallback directo) pasan
        por `is_new_id()` antes de procesar. Si el ID ya fue visto → descartado.
      - En la carga inicial los 20 IDs se registran en seen_ids sin procesar
        en tiempo real, evitando que el polling los reprocese.

    Flujo:
      1. Primera conexión → carga 20 giros iniciales de la API (sin DB, sin señales).
      2. Polling paralelo de 1s → garantiza datos frescos para ruletas rápidas.
      3. Cualquier giro nuevo detectado pasa por is_new_id() antes de on_number().
    """
    reconnect_delay = 5
    initial_loaded = False   # True tras carga inicial de los primeros 20

    # ── Deduplicación por ID único de jugada ──────────────────────────────────
    seen_ids: set  = set()
    seen_ids_queue: deque = deque(maxlen=200)   # límite de memoria: máx 200 IDs

    def is_new_id(gid: str) -> bool:
        if not gid or gid in seen_ids:
            return False
        if len(seen_ids_queue) == seen_ids_queue.maxlen:
            seen_ids.discard(seen_ids_queue[0])
        seen_ids.add(gid)
        seen_ids_queue.append(gid)
        return True

    while True:
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=40, close_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe", "key": ws_key, "casinoId": CASINO_ID
                }))
                logger.info(f"[WS-{ws_key}] ✅ Conectado | polling 1s | dedup activo")
                reconnect_delay = 5

                # ── Tarea paralela: re-suscribe cada 1 segundo ────────────────
                async def poll_1s():
                    while True:
                        await asyncio.sleep(1)
                        try:
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "key": ws_key,
                                "casinoId": CASINO_ID
                            }))
                        except Exception:
                            break

                poll_task = asyncio.create_task(poll_1s())

                try:
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue

                        results = data.get("last20Results")
                        if results and isinstance(results, list):

                            # ── CARGA INICIAL ─────────────────────────────────
                            if not initial_loaded:
                                initial_loaded = True
                                engine = next(
                                    (e for e in session_mgr.engines if e.ws_key == ws_key), None
                                )
                                loaded_count = 0
                                if engine:
                                    for item in reversed(results):
                                        gid_init = str(item.get("gameId", ""))
                                        if gid_init:
                                            if len(seen_ids_queue) == seen_ids_queue.maxlen:
                                                seen_ids.discard(seen_ids_queue[0])
                                            seen_ids.add(gid_init)
                                            seen_ids_queue.append(gid_init)
                                        try:
                                            n = int(item.get("result", ""))
                                        except (ValueError, TypeError):
                                            continue
                                        if 0 <= n <= 36:
                                            engine._update_state(n, persist=False, train_model=True)
                                            loaded_count += 1
                                    engine._train_models()
                                    if not engine.warmup_done and len(engine.spin_history) >= WARMUP_SPINS:
                                        engine.warmup_done = True
                                        engine.ws_count = len(engine.spin_history)
                                        logger.info(f"[WS-{ws_key}] ✅ WARMUP alcanzado tras carga inicial")
                                    logger.info(
                                        f"[WS-{ws_key}] 📦 {loaded_count} giros iniciales | "
                                        f"Historia: {len(engine.spin_history)} | "
                                        f"IDs registrados: {len(seen_ids)} | "
                                        f"Warmup: {'✅' if engine.warmup_done else '⏳'}"
                                    )
                                continue

                            # ── TIEMPO REAL: last20Results ────────────────────
                            latest = results[0]
                            gid = str(latest.get("gameId", ""))
                            if not is_new_id(gid):
                                continue
                            try:
                                n = int(latest.get("result", ""))
                            except (ValueError, TypeError):
                                continue
                            if 0 <= n <= 36:
                                session_mgr.on_number(ws_key, n)
                            continue

                        # ── Fallback: mensajes con número directo ─────────────
                        fallback_gid = str(data.get("gameId", "")).strip()
                        if not fallback_gid:
                            for key in ("result", "number", "outcome", "winningNumber"):
                                if key in data:
                                    fallback_gid = f"{ws_key}_{data[key]}_{int(time.time())}"
                                    break
                        if not fallback_gid or not is_new_id(fallback_gid):
                            continue

                        for key in ("result", "number", "outcome", "winningNumber"):
                            if key in data:
                                try:
                                    n = int(data[key])
                                    if 0 <= n <= 36:
                                        session_mgr.on_number(ws_key, n)
                                except (ValueError, TypeError):
                                    pass
                                break

                finally:
                    poll_task.cancel()
                    try:
                        await poll_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logger.warning(f"[WS-{ws_key}] Desconectado: {e}. Reconectando en {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)


# ─── FLASK ────────────────────────────────────────────────────────────────────
app = Flask(__name__)
session_mgr_global: Optional[SessionManager] = None

@app.route("/")
def home():
    return jsonify({"status": "ok", "bot": "Multi-Roulette Session Bot"})

@app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})

@app.route("/health")
def health():
    if not session_mgr_global:
        return jsonify({"status": "initializing"})
    active = session_mgr_global.engines[session_mgr_global.current_idx]
    in_session = SessionManager._in_vip_session()
    return jsonify({
        "active_roulette": active.name,
        "vip_session": in_session,
        "signal_sent": session_mgr_global.signal_sent_this_session,
        "signal_active": active.signal_active,
        "oportunidad_alerta": active.oportunidad_alerta,
        "engines": [
            {"name": e.name, "spins": len(e.spin_history), "warmup": e.warmup_done}
            for e in session_mgr_global.engines
        ]
    })

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url: return
    await asyncio.sleep(30)
    while True:
        try: urllib.request.urlopen(f"{url}/ping", timeout=15)
        except: pass
        await asyncio.sleep(240)

async def daily_stats_loop():
    """Envía estadísticas a las 00:00 hs de Argentina (UTC-3) cada día.
    Incluye stats del día que termina + stats globales acumuladas.
    Luego reinicia los contadores del día."""
    import datetime
    ARG_UTC_OFFSET = -3
    while True:
        now_utc = datetime.datetime.utcnow()
        now_arg = now_utc + datetime.timedelta(hours=ARG_UTC_OFFSET)
        target  = now_arg.replace(hour=0, minute=0, second=0, microsecond=0)
        if now_arg >= target:
            target += datetime.timedelta(days=1)
        wait_secs = (target - now_arg).total_seconds()
        logger.info(f"[Stats] Próximo reporte diario en {wait_secs/3600:.1f}h")
        await asyncio.sleep(wait_secs)
        if session_mgr_global:
            tg_send_stats(GLOBAL_STATS.get_midnight_text())
            GLOBAL_STATS._reset_day()
            logger.info("[Stats] Reporte 00:00 enviado al tema 40034. Contadores del día reiniciados.")

# ─── BOT COMMANDS ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎰 Multi-Roulette Session Bot — Sesiones VIP</b>\n\n"
        "Sesiones: 1-3 | 5-7 | 9-11 | 13-15 | 17-19 | 21-23 (ARG)\n"
        "Cada sesión → una ruleta diferente (5 ruletas, 6 sesiones)\n"
        "📊 Stats enviadas a las 00:00 hs (ARG)\n\n"
        "/status — Estado actual\n"
        "/stats — Ver estadísticas ahora\n"
        "/siguiente — Forzar cambio de ruleta",
        parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not session_mgr_global: return
    active = session_mgr_global.engines[session_mgr_global.current_idx]
    in_session = SessionManager._in_vip_session()
    if not in_session:
        st = "💤 Fuera de sesión VIP"
    elif active.signal_active:
        st = f"🟢 Señal activa: {active.active_pair}"
    elif active.oportunidad_alerta:
        st = "🚨 Oportunidad en evaluación"
    else:
        st = "⚪ Esperando señal"
    bot.reply_to(m,
        f"<b>Ruleta activa:</b> {active.name}\n"
        f"<b>Estado:</b> {st}\n"
        f"<b>Sesión VIP:</b> {'✅ Activa' if in_session else '❌ Inactiva'}\n"
        f"<b>Señal enviada:</b> {'✅' if session_mgr_global.signal_sent_this_session else '⏳'}",
        parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not session_mgr_global: return
    tg_send_stats(GLOBAL_STATS.get_midnight_text())

@bot.message_handler(commands=['siguiente'])
def cmd_siguiente(m):
    if not session_mgr_global: return
    session_mgr_global._advance_session()
    active = session_mgr_global.engines[session_mgr_global.current_idx]
    bot.reply_to(m, f"🔄 Cambiado a: <b>{active.name}</b>", parse_mode="HTML")

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global session_mgr_global
    session_mgr_global = SessionManager()

    threading.Thread(
        target=lambda: bot.polling(none_stop=True, interval=1, timeout=30),
        daemon=True
    ).start()

    tasks = [
        asyncio.create_task(session_mgr_global.session_watchdog()),
        asyncio.create_task(daily_stats_loop()),
    ]
    for r in ROULETTES:
        tasks.append(asyncio.create_task(ws_reader(r["key"], session_mgr_global)))
    tasks.append(asyncio.create_task(self_ping_loop()))

    logger.info("[Main] 🎰 Multi-Roulette Session Bot iniciado — Sesiones VIP por horario / rotación por sesión")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[Main] 🛑 Bot detenido")
