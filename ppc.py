#!/usr/bin/env python3
"""
Multi-Roulette Session Bot — 5 Ruletas / Sesiones de 30 min / 48 señales/día

Ruletas:
  - Roulette Italia Tricolore  — key 223
  - Roulette Deutsche          — key 222
  - Speed Roulette 1           — key 203
  - Speed Roulette 2           — key 205
  - Roulette 1 (Azure)         — key 227

Lógica:
  - Cada sesión dura 30 min (slot de tiempo fijo).
  - Las 5 ruletas se rotan en orden. Al terminar la 5ª, vuelve a la 1ª.
  - Cada sesión emite como máximo UNA señal.
  - Si en los 30 min no se detecta señal, envía mensaje de cambio a la siguiente ruleta.
  - Apuesta: 0.50 por categoría (total 1.00). En gale x3 → 1.50 c/u (total 3.00). Solo 1 gale.
  - No hay niveles de gestión, solo el nivel 1 (base).

CAMBIOS:
  - Al enviar mensaje de inicio de sesión (:00/:30) se borra el anterior de inicio Y el de fin.
  - Al enviar mensaje de fin de sesión (:25/:55) NO se borra el anterior (se borrará al iniciar la próxima).
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
    {"key": 223, "name": "ROULETTE ITALIA"},
    {"key": 222, "name": "ROULETTE DEUTSCHE"},
    {"key": 203, "name": "SPEED ROULETTE 1"},
    {"key": 205, "name": "SPEED ROULETTE 2"},
    {"key": 227, "name": "ROULETTE 1"},
    {"key": 204, "name": "MEGA ROULETTE"},
    {"key": 206, "name": "ROULETTE MACAO"},
]

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
WS_URL         = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID      = "ppcjd00000007254"
SESSION_ACTIVE = 25 * 60    # 25 min de señales activas
SESSION_PAUSE  = 5  * 60    # 5 min de pausa entre sesiones
SESSION_TOTAL  = SESSION_ACTIVE + SESSION_PAUSE   # 30 min = ciclo exacto por hora
BASE_BET       = 0.50
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
        self.signals_processed = 0

    def record(self, result_type: str, attempt: int, number: int,
               val, type_str: str, roulette_name: str, bankroll: float):
        self.signals_processed += 1
        if result_type == 'WIN':
            self.wins += 1; self.consecutive += 1
        elif result_type == 'LOSS':
            self.losses += 1; self.consecutive = 0
        elif result_type == 'EMPATE':
            self.zeros += 1
        self.last_20.append({
            "result": result_type, "attempt": attempt,
            "number": number, "val": val, "type": type_str,
            "roulette": roulette_name, "balance": bankroll
        })

    def get_stats_text(self, total_bankroll: float) -> str:
        total = self.wins + self.zeros + self.losses
        eff = ((self.wins + self.zeros) / total * 100) if total > 0 else 0.0
        text  = "📊 RESUMEN DIARIO — TODAS LAS RULETAS 📊\n"
        text += f"🕛 Reporte 12:00 hs (Argentina)\n\n"
        text += f"► PLACAR = ✅{self.wins} | 🟠{self.zeros} | 🚫{self.losses}\n"
        text += f"► Consecutivas = {self.consecutive}\n"
        text += f"► Assertividade = {eff:.2f}%\n"
        text += f"► Balance total: 💰 {total_bankroll:.2f}\n"
        text += f"► Total señales del día: {total}\n\n"
        text += "📌 Últimas 20 SEÑALES 📌\n"
        for s in reversed(list(self.last_20)):
            a_str = f"🔄 GALE #{s['attempt']}"
            b_str = f"💰 {s['balance']:.2f}"
            rl    = s['roulette'][:14]
            if s['result'] == 'WIN':
                text += f"✅ WIN #{s['number']} {s['type']} {s['val']} | {rl} | {a_str} | {b_str}\n"
            elif s['result'] == 'EMPATE':
                text += f"🟠 EMPATE #0 ZERO | {rl} | {a_str} | {b_str}\n"
            else:
                text += f"🚫 LOSS #{s['number']} {s['type']} {s['val']} | {rl} | {a_str} | {b_str}\n"
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
        self.total_signal_loss = 0.0
        self.oportunidad = 1         # 1 = entrada base, 2 = gale x3
        self.bankroll: float = 0.0
        self.active_signal_msg_id = None
        self.skip_next_spin = False   # True = ignorar el giro en que se envió la señal
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
        if not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None
        counts = self.after_number_dozen.get(last_num, {}) if cat_type == "DOCENA" else \
                 self.after_number_column.get(last_num, {})
        total = sum(counts.values())
        if total < 10: return None
        sc = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        if len(sc) < 2: return None
        missing = list({1, 2, 3} - {sc[0][0], sc[1][0]})[0]
        return {"pair": tuple(sorted([sc[0][0], sc[1][0]])), "missing": missing,
                "prob": (sc[0][1] + sc[1][1]) / total}

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
        type_str, singular = ("docenas", "docena") if self.active_type == "DOCENA" else ("columnas", "columna")
        gale_num  = self.oportunidad - 1   # 0 = entrada base, 1 = gale
        bet       = BASE_BET if gale_num == 0 else BASE_BET * 3
        total_bet = bet * 2
        return (
            f"✅✅ ENTRADA CONFIRMADA ✅✅\n\n"
            f"🕹️ {self.name}\n"
            f"🎯 Entrar en las {type_str}: {pair_disp}\n"
            f"💰 Balance: {self.bankroll:.2f}\n"
            f"💵 Apuesta total: {total_bet:.2f}\n"
            f"🚨 (por {singular}: {bet:.2f})\n"
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
        self.total_signal_loss = 0.0
        self.skip_next_spin   = True   # ignorar el giro actual, verificar desde el siguiente
        self.send_signal()

    def resolve(self, number: int) -> bool:
        """Resolver resultado. Retorna True si la señal terminó."""
        color    = REAL_COLOR_MAP.get(number, "VERDE")
        d, c     = get_dozen(number), get_column(number)
        type_str = self.active_type
        val_num  = d if type_str == "DOCENA" else c
        intento  = self.oportunidad   # 1 o 2
        bet      = BASE_BET if intento == 1 else BASE_BET * 3

        gale_num = intento - 1  # 0 = entrada base, 1 = gale

        # ── CERO ──────────────────────────────────────────────────────────────
        if number == 0:
            tg_send(
                f"🟠 EMPATE 0 — ZERO — 🔄 GALE #{gale_num}\n"
                f"🉑 Para la próxima ganaremos 0.00 🉑\n"
                f"💰 Balance actual: {self.bankroll:.2f}"
            )
            GLOBAL_STATS.record('EMPATE', intento, 0, 0, type_str, self.name, self.bankroll)
            self._reset_signal()
            return True

        won = (type_str == "DOCENA"  and d != 0 and f"D{d}" in self.active_pair) or \
              (type_str == "COLUMNA" and c != 0 and f"C{c}" in self.active_pair)

        if won:
            profit = bet
            self.bankroll = round(self.bankroll + profit, 2)
            cat_label = f"{'DOCENA' if type_str == 'DOCENA' else 'COLUMNA'} {val_num}"
            tg_send(
                f"✅ WIN {number} — {cat_label} — 🔄 GALE #{gale_num}\n"
                f"🎉 Felicidades has ganado {profit:.2f} 🎉\n"
                f"💰 Balance actual: {self.bankroll:.2f}"
            )
            GLOBAL_STATS.record('WIN', intento, number, val_num, type_str, self.name, self.bankroll)
            self._reset_signal()
            return True

        else:
            loss = bet * 2
            self.bankroll          = round(self.bankroll - loss, 2)
            self.total_signal_loss = round(self.total_signal_loss + loss, 2)

            if intento == 1:
                # ── Perdió Gale #0 → borrar mensaje de señal, enviar GALE #1 ──
                # El próximo giro valida Gale #1 directamente (NO se salta)
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None
                self.oportunidad    = 2
                self.send_signal()   # nuevo mensaje con GALE #1
                return False         # señal sigue activa

            else:
                # ── Perdió Gale #1 → mensaje con LOSS ──────────────────────
                cat_label = f"{'DOCENA' if type_str == 'DOCENA' else 'COLUMNA'} {val_num}"
                tg_send(
                    f"❌ LOSS {number} — {cat_label} — 🔄 GALE #{gale_num}\n"
                    f"🚨 Señal perdida. Monto total perdido en las 2 entradas: -{self.total_signal_loss:.2f} 🚨\n"
                    f"💰 Balance actual: {self.bankroll:.2f}"
                )
                GLOBAL_STATS.record('LOSS', 2, number, val_num, type_str, self.name, self.bankroll)
                self._reset_signal()
                return True

    def _reset_signal(self):
        self.signal_active = False
        self.active_pair = ()
        self.active_type = None
        self.total_signal_loss = 0.0
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
    Sesiones sincronizadas al reloj:
      - Arrancan en HH:00 y HH:30 exactos (hora Argentina UTC-3)
      - 25 min activos (señales) + 5 min de pausa → 48 sesiones/día
      - 5 ruletas rotando en orden. Al terminar la 5ª vuelve a la 1ª.
      - Solo la ruleta activa puede emitir señal; las demás acumulan datos.

    MENSAJES:
      - Al enviar mensaje de inicio (:00/:30) se elimina el anterior de inicio Y el de fin.
      - Al enviar mensaje de fin (:25/:55) solo se guarda el ID (se borrará al iniciar la próxima).
    """
    ARG_UTC_OFFSET = -3

    def __init__(self):
        self.engines: list[RouletteEngine] = [
            RouletteEngine(r["key"], r["name"]) for r in ROULETTES
        ]
        self.current_idx       = 0
        self.session_start     = 0.0      # epoch del inicio del slot actual
        self.session_active    = False    # True durante los 25 min activos
        self.signal_sent_this_session = False

        # ── IDs para borrar mensajes anteriores ───────────────────────────────
        self.prev_start_msg_id: Optional[int] = None   # msg de :00/:30 anterior
        self.prev_end_msg_id:   Optional[int] = None   # msg de :25/:55 anterior

        logger.info("[SessionManager] Iniciado — esperando próximo slot en punto o media hora.")

    # ── Hora Argentina ────────────────────────────────────────────────────────
    def _now_arg(self):
        import datetime
        return datetime.datetime.utcnow() + datetime.timedelta(hours=self.ARG_UTC_OFFSET)

    # ── Calcular cuántos segundos faltan para el próximo slot (HH:00 o HH:30) ─
    def seconds_to_next_slot(self) -> float:
        import datetime
        now = self._now_arg()
        if now.second <= 5 and now.minute in (0, 30):
            return 0.0
        if now.minute < 30:
            target = now.replace(minute=30, second=0, microsecond=0)
        else:
            target = (now + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        diff = (target - now).total_seconds()
        return max(0.0, diff)

    # ── Calcular el índice de ruleta según el slot del día ───────────────────
    def _slot_index_for_now(self) -> int:
        now = self._now_arg()
        slot_of_day = now.hour * 2 + (1 if now.minute >= 30 else 0)
        return slot_of_day % len(self.engines)

    # ── Iniciar sesión activa ─────────────────────────────────────────────────
    def _start_session(self):
        import datetime
        self.current_idx   = self._slot_index_for_now()
        self.session_start = time.time()
        self.session_active = True
        self.signal_sent_this_session = False
        engine = self.engines[self.current_idx]
        now_str = self._now_arg().strftime("%H:%M")
        end_str = (self._now_arg() + datetime.timedelta(minutes=25)).strftime("%H:%M")
        logger.info(f"[SessionManager] 🟢 Sesión iniciada: {engine.name} | {now_str}–{end_str} (ARG)")

        # ── Borrar mensajes anteriores de inicio y fin ────────────────────
        if self.prev_start_msg_id:
            tg_delete(CHAT_ID, self.prev_start_msg_id)
            self.prev_start_msg_id = None
        if self.prev_end_msg_id:
            tg_delete(CHAT_ID, self.prev_end_msg_id)
            self.prev_end_msg_id = None

        msg_id = tg_send(f"🔔 SESION INICIADA — {engine.name} 🔔")
        self.prev_start_msg_id = msg_id

    # ── Cerrar sesión activa (entra en pausa 5 min) ───────────────────────────
    def _end_session(self):
        import datetime
        engine = self.engines[self.current_idx]
        next_idx  = (self.current_idx + 1) % len(self.engines)
        next_name = self.engines[next_idx].name
        logger.info(f"[SessionManager] ⏸ Sesión terminada: {engine.name} | Pausa 5 min.")
        self.session_active = False

        msg_id = tg_send(
            f"⏸ SESIÓN CERRADA — {engine.name}\n"
            f"🎰 PRÓXIMA RULETA — {next_name} 🎰\n\n"
            f"💵 ¿COMO OPERAR LAS SEÑALES?\n\n"
            f"1° Op. = $0.50 USD x Docena/Columna\n"
            f"2° Op. = $1.50 USD x Docena/Columna\n\n"
            f"🎯 FUNCIONAMIENTO DE LAS SEÑALES 🎯\n\n"
            f"  • Se envía señal → Se resuelve\n"
            f"  • Sesión se cierra → Ej: 12:25 o 12:55\n"
            f"  • Nueva Sesión → Ej: 15:00 o 15:30\n"
            f"  • Nueva Señal → Ciclo de señales\n\n"
            f"♦️ POR SESION SE ENVÍA 1 SEÑAL ♦️"
        )
        self.prev_end_msg_id = msg_id

    # ── Watchdog: controla inicio/fin de sesión según el reloj ───────────────
    async def session_watchdog(self):
        # Esperar al primer slot
        wait = self.seconds_to_next_slot()
        logger.info(f"[SessionManager] ⏳ Esperando {wait/60:.1f} min para el primer slot...")
        await asyncio.sleep(wait)
        self._start_session()

        # Timestamp en que el watchdog detectó que la sesión debía terminar
        # pero había una señal activa. None = no estamos en espera.
        _waiting_signal_since: Optional[float] = None
        # Máximo tiempo extra que esperamos a que la señal se resuelva (2 spins = ~60s).
        SIGNAL_WAIT_TIMEOUT = 120  # segundos

        while True:
            await asyncio.sleep(1)
            now     = time.time()
            elapsed = now - self.session_start
            engine  = self.engines[self.current_idx]

            if self.session_active:
                if elapsed >= SESSION_ACTIVE:
                    if engine.signal_active:
                        # Registrar cuándo empezamos a esperar
                        if _waiting_signal_since is None:
                            _waiting_signal_since = now
                            logger.info(
                                f"[SessionManager] ⏳ Sesión terminada pero señal activa en "
                                f"{engine.name} — esperando resolución (máx {SIGNAL_WAIT_TIMEOUT}s)..."
                            )
                        # Si se superó el timeout, cancelar la señal forzosamente
                        elif now - _waiting_signal_since >= SIGNAL_WAIT_TIMEOUT:
                            logger.warning(
                                f"[SessionManager] ⚠️ Timeout esperando señal en {engine.name} "
                                f"({SIGNAL_WAIT_TIMEOUT}s). Cancelando señal y cerrando sesión."
                            )
                            if engine.active_signal_msg_id:
                                tg_edit(CHAT_ID, engine.active_signal_msg_id,
                                        engine._build_signal_text() +
                                        "\n\n⚠️ Señal cancelada — tiempo de sesión agotado.")
                            engine._reset_signal()
                            _waiting_signal_since = None
                            # Caer al cierre normal abajo
                        else:
                            continue  # Seguir esperando
                    else:
                        _waiting_signal_since = None  # señal resuelta, reset

                    # Cerrar sesión
                    end_time = time.time()
                    self._end_session()
                    # Calcular pausa restante desde el inicio del slot actual
                    pause_remaining = SESSION_TOTAL - (end_time - self.session_start)
                    if pause_remaining > 0:
                        logger.info(f"[SessionManager] ⏸ Pausa {pause_remaining:.0f}s")
                        await asyncio.sleep(pause_remaining)
                    self._start_session()
            else:
                if elapsed >= SESSION_TOTAL:
                    self._start_session()

    # ── Tick de sesión activa ─────────────────────────────────────────────────
    def tick_active(self, engine: RouletteEngine, number: int):
        engine.feed_number(number, active=True)

        if not self.session_active:
            return

        elapsed = time.time() - self.session_start
        if elapsed >= SESSION_ACTIVE:
            return

        if engine.signal_active:
            if engine.skip_next_spin:
                engine.skip_next_spin = False
                return
            done = engine.resolve(number)
            if done:
                elapsed2 = time.time() - self.session_start
                if elapsed2 >= SESSION_ACTIVE:
                    self._end_session()
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
        self._end_session()
        self.current_idx = (self.current_idx + 1) % len(self.engines)
        self.session_start = time.time()
        self.session_active = True
        self.signal_sent_this_session = False


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
    remaining = max(0, SESSION_ACTIVE - elapsed)
    return jsonify({
        "active_roulette": active.name,
        "session_elapsed_s": elapsed,
        "session_remaining_s": remaining,
        "signal_sent": session_mgr_global.signal_sent_this_session,
        "signal_active": active.signal_active,
        "engines": [
            {"name": e.name, "spins": len(e.spin_history), "warmup": e.warmup_done,
             "balance": e.bankroll}
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
            total_balance = sum(e.bankroll for e in session_mgr_global.engines)
            tg_send_stats(GLOBAL_STATS.get_stats_text(total_balance))
            logger.info("[Stats] Reporte diario enviado al tema 40034.")

# ─── BOT COMMANDS ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎰 Multi-Roulette Session Bot</b>\n\n"
        "5 ruletas rotando cada 30 min\n"
        "1 señal por sesión | 48 señales/día\n"
        "📊 Stats unificadas enviadas a las 12:00 hs (ARG)\n\n"
        "/status — Estado actual\n"
        "/stats — Ver estadísticas ahora\n"
        "/siguiente — Forzar cambio de ruleta",
        parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not session_mgr_global: return
    active = session_mgr_global.engines[session_mgr_global.current_idx]
    elapsed = int(time.time() - session_mgr_global.session_start)
    remaining = max(0, SESSION_ACTIVE - elapsed) // 60
    st = f"🟢 Señal activa: {active.active_pair}" if active.signal_active else "⚪ Esperando señal"
    bot.reply_to(m,
        f"<b>Ruleta activa:</b> {active.name}\n"
        f"<b>Estado:</b> {st}\n"
        f"<b>Tiempo restante:</b> {remaining} min\n"
        f"<b>Señal enviada:</b> {'✅' if session_mgr_global.signal_sent_this_session else '⏳'}\n"
        f"<b>Balance:</b> {active.bankroll:.2f}",
        parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not session_mgr_global: return
    total_balance = sum(e.bankroll for e in session_mgr_global.engines)
    tg_send_stats(GLOBAL_STATS.get_stats_text(total_balance))

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

    logger.info("[Main] 🎰 Multi-Roulette Session Bot iniciado — 5 ruletas / 30 min por sesión")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[Main] 🛑 Bot detenido")
