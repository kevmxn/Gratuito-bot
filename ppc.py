#!/usr/bin/env python3
"""
Multi-Roulette Session Bot — 5 Ruletas / Rotación por señal ganada

Ruletas:
  - Turkish Roulette   — key 224
  - Russian Roulette   — key 221
  - Roulette Macao     — key 206
  - Roulette 2         — key 201
  - Auto Roulette      — key 225

Lógica:
  - NO hay sesiones de tiempo fijo de 30 min.
  - La ruleta rota automáticamente luego de ganar (WIN) una señal.
  - Tras el WIN se espera 5 minutos antes de iniciar en la siguiente ruleta.
  - Cada sesión (ruleta activa) emite como máximo UNA señal.
  - Si no se detecta señal, la ruleta permanece activa indefinidamente hasta emitir una.
  - Apuesta: 0.50 por categoría (total 1.00). En gale x3 → 1.50 c/u (total 3.00). Solo 1 gale.
  - Estadísticas históricas ponderadas exponencialmente (más peso a los datos recientes).

MENSAJES:
  - Al enviar mensaje de inicio de sesión se borra el anterior de inicio Y el de fin.
  - Al enviar mensaje de fin de sesión NO se borra el anterior (se borrará al iniciar la próxima).
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
    "MEGA ROULETTE":          "https://1win.lat/casino/play/v_pragmatic:megaroulette",
    "AUTO ROULETTE":          "https://1win.lat/casino/play/v_pragmatic:1winautoroulette",
    "ROULETTE ITALIA":        "https://1win.lat/casino/play/v_pragmatic:rouletteitaliatricolore",
    "ROULETTE 1":             "https://1win.lat/casino/play/v_pragmatic:roulette1",
    "ROULETTE 2":             "https://1win.lat/casino/play/v_pragmatic:roulette2",
    "ROULETTE 2 EXTRA TIME":  "https://1win.lat/casino/play/v_pragmatic:roulette2",
    "SPEED ROULETTE 1":       "https://1win.lat/casino/play/v_pragmatic:speedroulette1",
    "SPEED ROULETTE 2":       "https://1win.lat/casino/play/v_pragmatic:speedroulette2",
    "ROULETTE MACAO":         "https://1win.lat/casino/play/v_pragmatic:roulettemacao",
    "ROULETTE LATINA":        "https://1win.lat/casino/play/v_pragmatic:1winspanishroulette",
    "ROULETTE DEUTSCHE":      "https://1win.lat/casino/play/v_pragmatic:germanroulette",
    "TURKISH ROULETTE":       "https://1win.lat/casino/play/v_pragmatic:1winturkishroulette",
    "RUSSIAN ROULETTE":       "https://1win.lat/casino/play/v_pragmatic:1winroulette",
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
WIN_ROTATE_PAUSE = 10 * 60   # 10 min de espera tras resolver señal antes de cambiar ruleta
WARMUP_SPINS   = 25
MIN_PROB       = 0.78
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
    """Estadísticas unificadas de todas las ruletas. Se envía una vez al día a las 12:00 ARG."""

    def __init__(self):
        self.wins = 0; self.zeros = 0; self.losses = 0
        self.consecutive = 0
        self.last_20 = deque(maxlen=20)

    def record(self, result_type: str, attempt: int, number: int,
               val, type_str: str, roulette_name: str):
        if result_type == 'WIN':
            self.wins += 1; self.consecutive += 1
        elif result_type == 'LOSS':
            self.losses += 1; self.consecutive = 0
        elif result_type == 'EMPATE':
            self.zeros += 1
        self.last_20.append({
            "result": result_type, "attempt": attempt,
            "number": number, "val": val, "type": type_str,
            "roulette": roulette_name,
        })

    def get_stats_text(self) -> str:
        total = self.wins + self.zeros + self.losses
        eff = ((self.wins + self.zeros) / total * 100) if total > 0 else 0.0
        text  = "📆 MARCADOR DIARIO\n"
        text += f"⚪ PLACAR = ✅{self.wins} | 🟠{self.zeros} | 🚫{self.losses}\n"
        text += f"⚪ Consecutivas = {self.consecutive}\n"
        text += f"⚪ Assertividade = {eff:.2f}%\n"
        text += f"⚪ Total señales del día: {total}\n\n"
        text += "📌 ÚLTIMAS 20 SEÑALES 📌\n"
        for s in reversed(list(self.last_20)):
            gale_str = f"🔄 GALE #{s['attempt'] - 1}"
            if s['result'] == 'WIN':
                text += f"✅ WIN #{s['number']} {s['type']} {s['val']} | {gale_str}\n"
            elif s['result'] == 'EMPATE':
                text += f"🟠 EMPATE #0 ZERO | {gale_str}\n"
            else:
                text += f"❌ LOSS #{s['number']} {s['type']} {s['val']} | {gale_str}\n"
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
        gale_num  = self.oportunidad - 1   # 0 = entrada base, 1 = gale
        return (
            f"✅✅ ENTRADA CONFIRMADA ✅✅\n\n"
            f"🕹️ {self.name}\n"
            f"🎯 Entrar en las {type_str}: {pair_disp}\n"
            f"⚔️ Cubrir el CERO 🟢\n"
            f"🛟 GALE #{gale_num}"
        )

    def send_signal(self):
        msg_id = tg_send(self._build_signal_text())
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
        intento  = self.oportunidad   # 1 o 2
        gale_num = intento - 1        # 0 = entrada base, 1 = gale

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
            if intento == 1:
                # Perdió Gale #0 → borrar señal, reenviar con GALE #1
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None
                self.oportunidad = 2
                self.send_signal()
                return False   # señal sigue activa

            else:
                # Perdió Gale #1 → LOSS
                cat_label = f"{'DOCENA' if type_str == 'DOCENA' else 'COLUMNA'} {val_num}"
                GLOBAL_STATS.record('LOSS', 2, number, val_num, type_str, self.name)
                tg_send(f"❌ LOSS {number} — {cat_label} — 🔄 GALE #{gale_num}")
                self._reset_signal()
                return True

    def _reset_signal(self):
        self.signal_active = False
        self.active_pair = ()
        self.active_type = None
        self.oportunidad = 1
        self.active_signal_msg_id = None

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
    Rotación por señal ganada:
      - Sin sesiones de tiempo fijo. La ruleta activa permanece hasta emitir 1 señal.
      - Al resolver cualquier señal (WIN/LOSS/EMPATE), se espera WIN_ROTATE_PAUSE (10 min) y se rota.
      - En LOSS o EMPATE la sesión continúa en la misma ruleta (sin señal activa).
      - Solo la ruleta activa puede emitir señal; las demás acumulan datos en modo pasivo.

    MENSAJES:
      - Al iniciar sesión se elimina el mensaje de inicio anterior Y el de fin anterior.
      - Al enviar mensaje de fin solo se guarda el ID (se borrará al iniciar la próxima).
    """

    def __init__(self):
        self.engines: list[RouletteEngine] = [
            RouletteEngine(r["key"], r["name"]) for r in ROULETTES
        ]
        self.current_idx            = 0
        self.session_start          = time.time()
        self.session_active         = True
        self.signal_sent_this_session = False
        self.pending_rotation       = False   # True durante los 5 min de espera tras WIN
        self.rotation_start_time    = 0.0     # epoch de cuando empieza la espera

        # ── IDs para borrar mensajes anteriores ───────────────────────────────
        self.prev_start_msg_id: Optional[int] = None
        self.prev_end_msg_id:   Optional[int] = None

        logger.info("[SessionManager] Iniciado — rotación por señal ganada / 5 min de pausa post-WIN.")
        self._send_start_message()

    # ── Hora Argentina ────────────────────────────────────────────────────────
    def _now_arg(self):
        import datetime
        return datetime.datetime.utcnow() + datetime.timedelta(hours=-3)

    # ── Enviar mensaje de inicio de sesión ────────────────────────────────────
    def _send_start_message(self):
        engine = self.engines[self.current_idx]
        # Borrar inicio anterior + fin anterior
        if self.prev_start_msg_id:
            tg_delete(CHAT_ID, self.prev_start_msg_id)
            self.prev_start_msg_id = None
        if self.prev_end_msg_id:
            tg_delete(CHAT_ID, self.prev_end_msg_id)
            self.prev_end_msg_id = None
        msg_id = tg_send(f"🔔 SESION INICIADA — {engine.name} 🔔")
        self.prev_start_msg_id = msg_id
        logger.info(f"[SessionManager] 🟢 Sesión activa: {engine.name}")

    # ── Rotar a siguiente ruleta ──────────────────────────────────────────────
    def _rotate_to_next(self):
        """Cierra la sesión actual y pasa a la siguiente ruleta."""
        engine     = self.engines[self.current_idx]
        next_idx   = (self.current_idx + 1) % len(self.engines)
        next_name  = self.engines[next_idx].name

        logger.info(f"[SessionManager] 🔄 Rotando: {engine.name} → {next_name}")

        # Borrar fin anterior
        if self.prev_end_msg_id:
            tg_delete(CHAT_ID, self.prev_end_msg_id)
            self.prev_end_msg_id = None

        text = (
            f"⏸ SESIÓN CERRADA — {engine.name}\n"
            f"🎰 PRÓXIMA RULETA — {next_name} 🎰\n\n"
            f"🎯 FUNCIONAMIENTO DE LAS SEÑALES 🎯\n\n"
            f"  • Se envía señal → Se resuelve\n"
            f"  • Al ganar → Rotamos de ruleta (5 min de pausa)\n"
            f"  • Nueva Ruleta → Nueva Señal\n\n"
            f"♦️ POR SESIÓN SE ENVÍA 1 SEÑAL ♦️"
        )
        msg_id = tg_send_with_button(text, next_name)
        self.prev_end_msg_id = msg_id

        self.current_idx              = next_idx
        self.session_start            = time.time()
        self.session_active           = True
        self.signal_sent_this_session = False
        self._send_start_message()

    # ── Señal resuelta: iniciar espera de 10 min antes de rotar ─────────────
    def _on_signal_resolved(self):
        """Llamado al terminar cualquier señal (WIN/LOSS/EMPATE). Inicia la pausa de 10 min."""
        self.pending_rotation    = True
        self.rotation_start_time = time.time()
        remaining_min = WIN_ROTATE_PAUSE // 60
        engine = self.engines[self.current_idx]
        next_name = self.engines[(self.current_idx + 1) % len(self.engines)].name
        logger.info(
            f"[SessionManager] 🔄 Señal resuelta en {engine.name} — "
            f"esperando {remaining_min} min antes de rotar a {next_name}."
        )
        tg_send(
            f"⏳ Rotando ruleta en <b>{remaining_min} minutos</b>...\n"
            f"🎰 Próxima: <b>{next_name}</b>"
        )

    # ── Watchdog: solo vigila la rotación post-WIN ────────────────────────────
    async def session_watchdog(self):
        while True:
            await asyncio.sleep(1)
            if self.pending_rotation:
                elapsed = time.time() - self.rotation_start_time
                if elapsed >= WIN_ROTATE_PAUSE:
                    self.pending_rotation = False
                    self._rotate_to_next()

    # ── Tick de sesión activa ─────────────────────────────────────────────────
    def tick_active(self, engine: RouletteEngine, number: int):
        engine.feed_number(number, active=True)

        if not self.session_active or self.pending_rotation:
            return

        if engine.signal_active:
            done = engine.resolve(number)
            if done:
                # Señal resuelta (WIN / LOSS / EMPATE) → rotar ruleta en 10 min
                self._on_signal_resolved()
            return

        if not self.signal_sent_this_session and engine.warmup_done:
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
        self.pending_rotation = False
        self._rotate_to_next()



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
    elapsed = int(time.time() - session_mgr_global.session_start)
    pending = session_mgr_global.pending_rotation
    rotation_in = max(0, int(WIN_ROTATE_PAUSE - (time.time() - session_mgr_global.rotation_start_time))) \
                  if pending else None
    return jsonify({
        "active_roulette": active.name,
        "session_elapsed_s": elapsed,
        "signal_sent": session_mgr_global.signal_sent_this_session,
        "signal_active": active.signal_active,
        "pending_rotation": pending,
        "rotation_in_s": rotation_in,
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
    """Envía estadísticas unificadas a las 12:00 hs de Argentina (UTC-3) cada día."""
    import datetime
    ARG_UTC_OFFSET = -3
    while True:
        now_utc = datetime.datetime.utcnow()
        now_arg = now_utc + datetime.timedelta(hours=ARG_UTC_OFFSET)
        target  = now_arg.replace(hour=12, minute=0, second=0, microsecond=0)
        if now_arg >= target:
            target += datetime.timedelta(days=1)
        wait_secs = (target - now_arg).total_seconds()
        logger.info(f"[Stats] Próximo reporte diario en {wait_secs/3600:.1f}h")
        await asyncio.sleep(wait_secs)
        if session_mgr_global:
            tg_send_stats(GLOBAL_STATS.get_stats_text())
            logger.info("[Stats] Reporte diario enviado al tema 40034.")

# ─── BOT COMMANDS ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎰 Multi-Roulette Session Bot</b>\n\n"
        "5 ruletas — rotación automática tras WIN\n"
        "5 min de pausa entre ruletas\n"
        "📊 Stats unificadas enviadas a las 12:00 hs (ARG)\n\n"
        "/status — Estado actual\n"
        "/stats — Ver estadísticas ahora\n"
        "/siguiente — Forzar cambio de ruleta",
        parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not session_mgr_global: return
    active = session_mgr_global.engines[session_mgr_global.current_idx]
    elapsed = int(time.time() - session_mgr_global.session_start) // 60
    pending = session_mgr_global.pending_rotation
    if pending:
        rot_in = max(0, int(WIN_ROTATE_PAUSE - (time.time() - session_mgr_global.rotation_start_time)))
        st = f"⏳ Rotando en {rot_in}s → {session_mgr_global.engines[(session_mgr_global.current_idx+1)%len(session_mgr_global.engines)].name}"
    elif active.signal_active:
        st = f"🟢 Señal activa: {active.active_pair}"
    else:
        st = "⚪ Esperando señal"
    bot.reply_to(m,
        f"<b>Ruleta activa:</b> {active.name}\n"
        f"<b>Estado:</b> {st}\n"
        f"<b>Tiempo en esta ruleta:</b> {elapsed} min\n"
        f"<b>Señal enviada:</b> {'✅' if session_mgr_global.signal_sent_this_session else '⏳'}",
        parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not session_mgr_global: return
    tg_send_stats(GLOBAL_STATS.get_stats_text())

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

    logger.info("[Main] 🎰 Multi-Roulette Session Bot iniciado — 5 ruletas / rotación por WIN / 5 min pausa")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[Main] 🛑 Bot detenido")
