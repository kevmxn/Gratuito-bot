#!/usr/bin/env python3
"""
Russian Roulette — Bot de señales para Docenas y Columnas exclusivamente
Sistema PF + PH + ML Cruzado
  - PF (Frecuencia últimos 5): Top 2 D/C del bloque actual (Peso 65%)
  - PH (Histórico último nº): Top 2 D/C tras el último nº (Peso 35%)
  - ML (Markov + NB + SGD + AMX): Features cruzados (D,C) + PF + PH
  - Entrenamiento cada 100 giros. Umbral: 80%
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.request
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Tuple

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
logging.basicConfig(level=logging.INFO, format='%(asctime)s [RussianDC] %(levelname)s %(message)s')
logger = logging.getLogger("RussianDC")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN   = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"
CHAT_ID = -1003630680656  # Nuevo ID del canal

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET","POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
bot = telebot.TeleBot(TOKEN, threaded=False); bot.session = _session

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
WS_KEY    = 221
LIVE_DB   = "russian_live.db"
AZURE_DB  = "russian-azure.db"
AZURE_TABLE = "russian_roulette"

BASE_BET     = 0.50  # Apuesta base por docena/columna
MAX_ATTEMPTS = 2       # Gale 0 y Gale 1
WARMUP_SPINS = 25
MIN_PROB     = 0.78
TRAIN_INTERVAL = 100

REAL_COLOR_MAP: dict[int, str] = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO",
}
COLOR_EMOJI = {"ROJO":"🔴","NEGRO":"⚫️","VERDE":"🟢"}

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(LIVE_DB, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS live_spins ( id INTEGER PRIMARY KEY AUTOINCREMENT, number INTEGER NOT NULL, ts INTEGER NOT NULL)""")
    conn.commit(); return conn

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
            time.sleep(delay); delay = min(delay*2, 60)
    return None

def tg_send(text: str) -> Optional[int]:
    kwargs = dict(chat_id=CHAT_ID, text=text, parse_mode="HTML")
    msg = _tg_call(bot.send_message, **kwargs)
    return msg.message_id if msg else None

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if len(data) < period: return [None]*len(data)
    mult = 2 / (period + 1); out = [None]*(period-1); prev = sum(data[:period]) / period; out.append(prev)
    for v in data[period:]: prev = v*mult + prev*(1-mult); out.append(prev)
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
        return ((pe4 <= pe20 and ce4 > ce20) or (cur > ce4 and cur > ce8 and cur > ce20))
    else:
        v_pattern = False
        if len(levels) >= 3: a, b, c = levels[-3], levels[-2], levels[-1]; v_pattern = (b < a) and (b < c) and (c > a)
        return ((pe4 <= pe8 and ce4 > ce8) or (pe8 <= pe20 and ce8 > ce20) or (cur > ce4 and cur > ce8) or v_pattern)

# ─── MARKOV SUAVIZADO ─────────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window; self.order = order; self.transition_counts: dict = {}
    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1: return
        for i in range(len(recent) - self.order):
            state = tuple(recent[i:i+self.order]); nxt = recent[i+self.order]
            self.transition_counts[state][nxt] += 1
    def predict(self, sequence: list) -> Optional[dict]:
        if len(sequence) < self.order: return None
        state = tuple(sequence[-self.order:]); counts = dict(self.transition_counts.get(state, {})); total = sum(counts.values())
        if total < 10: return None
        alpha = 2.0; vocab_size = 3
        probs = {k: (v + alpha) / (total + alpha * vocab_size) for k,v in counts.items()}
        for c in [1,2,3]:
            if c not in probs: probs[c] = alpha / (total + alpha * vocab_size)
        return probs

# ─── ENSEMBLE ML CRUZADO (D+C+PF+PH Features) ───────────────────────────────
class OnlineEnsemblePredictor:
    WINDOW = 5; CLASSES = [1, 2, 3]
    def __init__(self):
        self.mnb = MultinomialNB(alpha=2.0, class_prior=[0.333, 0.333, 0.333])
        self.sgd = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.005, penalty='l2', alpha=0.01, epsilon=0.2)
        self.trained = False; self.sample_count = 0

    def _extract_features(self, hist_d: list, hist_c: list, pf_pair_d: tuple, ph_pair_d: tuple, pf_pair_c: tuple, ph_pair_c: tuple) -> Optional[list]:
        if len(hist_d) < self.WINDOW or len(hist_c) < self.WINDOW: return None
        features = []
        for i in range(1, self.WINDOW + 1):
            d = hist_d[-i]; c = hist_c[-i]
            pair_idx = (d - 1) * 3 + (c - 1)
            vec = [0]*9; vec[pair_idx] = 1
            features.extend(vec)
        d_vec = [0,0,0]
        for d in pf_pair_d: d_vec[d-1] = 1
        features.extend(d_vec)
        d_vec_ph = [0,0,0]
        for d in ph_pair_d: d_vec_ph[d-1] = 1
        features.extend(d_vec_ph)
        c_vec = [0,0,0]
        for c in pf_pair_c: c_vec[c-1] = 1
        features.extend(c_vec)
        c_vec_ph = [0,0,0]
        for c in ph_pair_c: c_vec_ph[c-1] = 1
        features.extend(c_vec_ph)
        return features

    def partial_train(self, hist_d: list, hist_c: list, target: int, pf_d: tuple, ph_d: tuple, pf_c: tuple, ph_c: tuple):
        feats = self._extract_features(hist_d[:-1], hist_c[:-1], pf_d, ph_d, pf_c, ph_c)
        if feats is None: return
        X = np.array(feats).reshape(1, -1); y = np.array([target])
        if not self.trained:
            self.mnb.partial_fit(X, y, classes=self.CLASSES); self.sgd.partial_fit(X, y, classes=self.CLASSES); self.trained = True
        else:
            self.mnb.partial_fit(X, y); self.sgd.partial_fit(X, y)
        self.sample_count += 1

    def predict(self, hist_d: list, hist_c: list, pf_d: tuple, ph_d: tuple, pf_c: tuple, ph_c: tuple) -> Optional[dict]:
        if not self.trained: return None
        feats = self._extract_features(hist_d, hist_c, pf_d, ph_d, pf_c, ph_c)
        if feats is None: return None
        X = np.array(feats).reshape(1, -1)
        try:
            nb_probs = self.mnb.predict_proba(X)[0]; sgd_probs = self.sgd.predict_proba(X)[0]
            final_probs = (0.5 * nb_probs + 0.5 * sgd_probs)
            return {c+1: float(p) for c, p in enumerate(final_probs)}
        except: return None

# ─── DETAILED STATS (MODIFICADO X2 y NUEVO FORMATO) ──────────────────────────
class DetailedStats:
    def __init__(self):
        self.wins = 0
        self.zeros = 0
        self.losses = 0
        self.consecutive = 0
        self.last_20 = deque(maxlen=20)
        self.signals_processed = 0
        self.last_report_signals = 0

    def record(self, result_type: str, attempt: int, number: int, val: int, type_str: str, bankroll: float):
        self.signals_processed += 1
        if result_type == 'WIN':
            self.wins += 1
            self.consecutive += 1
        elif result_type == 'LOSS':
            self.losses += 1
            self.consecutive = 0
        elif result_type == 'EMPATE':
            self.zeros += 1
            # Empate no rompe racha consecutiva de wins, pero tampoco suma. 
            # Si consideras que sí la rompe, descomenta la siguiente línea:
            # self.consecutive = 0

        self.last_20.append({
            "result": result_type,
            "attempt": attempt,
            "number": number,
            "val": val,
            "type": type_str,
            "balance": bankroll
        })

    def should_send(self) -> bool: 
        return (self.signals_processed - self.last_report_signals) >= 20

    def mark_sent(self): 
        self.last_report_signals = self.signals_processed

    def get_stats_text(self, bankroll: float) -> str:
        total = self.wins + self.zeros + self.losses
        eff = (self.wins / total * 100) if total > 0 else 0.0
        
        text = "📊 RESUMEN DE SEÑALES 📊\n"
        text += f"► PLACAR = ✅{self.wins} | 🟠{self.zeros} | 🚫{self.losses}\n"
        text += f"► Consecutivas = {self.consecutive}\n"
        text += f"► Assertividade = {eff:.2f}%\n"
        text += f"► Balance actual: 💰 {bankroll:.1f}\n"
        text += f"► Total señales procesadas: {total}\n\n"
        text += "📌 Últimas 20 SEÑALES 📌\n"
        
        for s in reversed(list(self.last_20)): # De la más nueva a la más vieja
            attempt_str = f"🔄 GALE #{s['attempt']}"
            balance_str = f"💰 {s['balance']:.1f}"
            if s['result'] == 'WIN':
                text += f"✅ WIN #{s['number']} {s['type']} {s['val']} | {attempt_str} | {balance_str}\n"
            elif s['result'] == 'EMPATE':
                text += f"🟠 EMPATE #0 ZERO | {attempt_str} | {balance_str}\n"
            else:
                text += f"🚫 LOSS #{s['number']} {s['type']} {s['val']} | {attempt_str} | {balance_str}\n"
                
        text += "\n| 🟠0 = son si la señal salió el cero\n"
        text += "| 🚫0 = si la señal se perdió en los 2 intentos\n\n"
        text += "► PLACAR = ✅X | 🟠Y | 🚫Z = Son las estadísticas de las 24 horas\n"
        text += "► Consecutivas = X = señales seguidas ganadas\n"
        text += "► Assertividade = X% = efectividad de las 24 horas\n"
        text += "► Total señales procesadas: X = señales procesadas en las 24 horas"
        return text

# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RussianRouletteEngine:
    def __init__(self):
        self.spin_history: list = []
        self.dozen_seq: list = []; self.column_seq: list = []
        self.d_levels: dict[int, list] = {1:[], 2:[], 3:[]}; self.c_levels: dict[int, list] = {1:[], 2:[], 3:[]}

        self.markov_d = SmoothedMarkovPredictor(window=60, order=2); self.markov_c = SmoothedMarkovPredictor(window=60, order=2)
        self.ensemble_d = OnlineEnsemblePredictor(); self.ensemble_c = OnlineEnsemblePredictor()

        self.after_number_dozen: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self.after_number_column: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))

        self.signal_active: bool = False; self.active_type: Optional[str] = None
        self.active_pair: tuple = (); self.active_missing: str = ""
        self.attempts_left: int = MAX_ATTEMPTS
        self.bankroll: float = 100000.0 # Balance inicial ajustado a tus ejemplos
        self.trigger_number: int = 0; self.trigger_color: str = ""

        self.stats = DetailedStats(); self._db = _get_db()
        self.spins_since_train: int = 0
        
        live_loaded = self._load_live_history(); azure_loaded = self._pretrain_from_db(AZURE_DB, AZURE_TABLE)
        total_preloaded = live_loaded + azure_loaded
        self.ws_count: int = total_preloaded; self.warmup_done: bool = total_preloaded >= WARMUP_SPINS
        self.last_game_id: Optional[str] = None
        
        logger.info(f"[RussianDC] 📦 Pre-cargados: {total_preloaded} | Warmup: {'✅' if self.warmup_done else '⏳'}")

    # Sistema de gestión x2
    def current_bet(self) -> float:
        level = MAX_ATTEMPTS - self.attempts_left # 0 para Gale 0, 1 para Gale 1
        return round(BASE_BET * (2 ** level), 2)

    def _pretrain_from_db(self, db_path: str, table_name: str) -> int:
        if not os.path.exists(db_path): return 0
        spins = []
        try:
            pattern = re.compile(rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),')
            with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = pattern.search(line)
                    if m: spins.append(int(m.group(1)))
        except: return 0
        if not spins: return 0
        for n in spins: self._update_state(n, persist=False, train_model=False)
        self._train_models()
        return len(spins)

    def _load_live_history(self) -> int:
        try: rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except: return 0
        if not rows: return 0
        for (n,) in rows: self._update_state(n, persist=False, train_model=False)
        self._train_models()
        return len(rows)

    def _persist(self, number: int):
        try: self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)", (number, int(time.time()))); self._db.commit()
        except: pass

    def _train_models(self):
        self.markov_d.update(self.dozen_seq); self.markov_c.update(self.column_seq)

    def _update_state(self, number: int, persist: bool = True, train_model: bool = True):
        color = REAL_COLOR_MAP.get(number, "VERDE"); d = get_dozen(number); c = get_column(number)
        if number != 0 and len(self.spin_history) >= 1:
            prev_num = self.spin_history[-1]["number"]
            if prev_num != 0:
                self.after_number_dozen[prev_num][d] += 1
                self.after_number_column[prev_num][c] += 1
        self.spin_history.append({"number":number,"color":color})
        if d != 0:
            self.dozen_seq.append(d)
            for dd in (1,2,3):
                delta = 1 if d == dd else -1; prev = self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev + delta)
        if c != 0:
            self.column_seq.append(c)
            for cc in (1,2,3):
                delta = 1 if c == cc else -1; prev = self.c_levels[cc][-1] if self.c_levels[cc] else 0
                self.c_levels[cc].append(prev + delta)
        if train_model and d != 0 and c != 0 and len(self.dozen_seq) > 5:
            pf_d, ph_d = self._get_pf("DOCENA"), self._get_ph("DOCENA")
            pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
            if pf_d and ph_d and pf_c and ph_c:
                self.ensemble_d.partial_train(self.dozen_seq, self.column_seq, d, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
                self.ensemble_c.partial_train(self.dozen_seq, self.column_seq, c, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
            self.spins_since_train += 1
            if self.spins_since_train >= TRAIN_INTERVAL:
                self._train_models(); self.spins_since_train = 0
                logger.info(f"[RussianDC] 🧠 Modelos re-entrenados (C/100 giros)")
        if persist: self._persist(number)

    def _get_pf(self, cat_type: str) -> Optional[Dict]:
        if len(self.spin_history) < 5: return None
        last5 = self.spin_history[-5:]
        counts = {1:0, 2:0, 3:0}
        for s in last5:
            n = s["number"]
            if n != 0:
                val = get_dozen(n) if cat_type == "DOCENA" else get_column(n)
                counts[val] += 1
        active = [k for k,v in counts.items() if v > 0]
        if len(active) != 2: return None
        missing = list({1,2,3} - set(active))[0]
        prob = (counts[active[0]] + counts[active[1]]) / 5.0
        return {"pair": tuple(sorted(active)), "missing": missing, "prob": prob}

    def _get_ph(self, cat_type: str) -> Optional[Dict]:
        if len(self.spin_history) == 0: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None
        counts = self.after_number_dozen.get(last_num, {}) if cat_type == "DOCENA" else self.after_number_column.get(last_num, {})
        total = sum(counts.values())
        if total < 10: return None
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_counts) < 2: return None
        missing = list({1,2,3} - set([sorted_counts[0][0], sorted_counts[1][0]]))[0]
        return {
            "pair": tuple(sorted([sorted_counts[0][0], sorted_counts[1][0]])),
            "missing": missing,
            "prob": (sorted_counts[0][1] + sorted_counts[1][1]) / total
        }

    def _predict_pair_ml(self, cat_type: str, missing_num: int) -> float:
        mk = self.markov_d if cat_type == "DOCENA" else self.markov_c
        hist = self.dozen_seq if cat_type == "DOCENA" else self.column_seq
        levels = (self.d_levels if cat_type == "DOCENA" else self.c_levels).get(missing_num, [])
        m_p_miss = mk.predict(hist).get(missing_num, 1/3) if mk.predict(hist) else 1/3
        pf_d, ph_d = self._get_pf("DOCENA"), self._get_ph("DOCENA")
        pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
        ens_p_miss = 1/3
        if pf_d and ph_d and pf_c and ph_c:
            ens_pred = self.ensemble_d.predict(hist, self.column_seq, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"]) if cat_type == "DOCENA" else \
                       self.ensemble_c.predict(self.dozen_seq, hist, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
            if ens_pred: ens_p_miss = ens_pred.get(missing_num, 1/3)
        ml_prob_missing = 0.4 * m_p_miss + 0.6 * ens_p_miss
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ml_prob_missing *= 0.85
            elif ema_signal(levels, "moderado"): ml_prob_missing *= 0.92
        return 1.0 - ml_prob_missing

    def _detect_signal(self) -> Optional[dict]:
        pf_d = self._get_pf("DOCENA"); pf_c = self._get_pf("COLUMNA")
        if not pf_d and not pf_c: return None
        ph_d = self._get_ph("DOCENA"); ph_c = self._get_ph("COLUMNA")
        candidates = []
        if pf_d and ph_d and set(pf_d["pair"]) == set(ph_d["pair"]):
            base_prob = (0.65 * pf_d["prob"]) + (0.35 * ph_d["prob"])
            ml_pair_prob = self._predict_pair_ml("DOCENA", pf_d["missing"])
            final_prob = (0.5 * base_prob) + (0.5 * ml_pair_prob)
            logger.info(f"[RussianDC] D PF:{pf_d['pair']}({pf_d['prob']:.0%}) PH:{ph_d['pair']}({ph_d['prob']:.0%}) Base:{base_prob:.0%} ML:{ml_pair_prob:.0%} Fin:{final_prob:.0%}")
            if final_prob >= MIN_PROB:
                candidates.append({"type":"DOCENA", "pair":tuple(f"D{x}" for x in sorted(pf_d["pair"])), "missing":f"D{pf_d['missing']}", "prob":final_prob})
        if pf_c and ph_c and set(pf_c["pair"]) == set(ph_c["pair"]):
            base_prob = (0.65 * pf_c["prob"]) + (0.35 * ph_c["prob"])
            ml_pair_prob = self._predict_pair_ml("COLUMNA", pf_c["missing"])
            final_prob = (0.5 * base_prob) + (0.5 * ml_pair_prob)
            logger.info(f"[RussianDC] C PF:{pf_c['pair']}({pf_c['prob']:.0%}) PH:{ph_c['pair']}({ph_c['prob']:.0%}) Base:{base_prob:.0%} ML:{ml_pair_prob:.0%} Fin:{final_prob:.0%}")
            if final_prob >= MIN_PROB:
                candidates.append({"type":"COLUMNA", "pair":tuple(f"C{x}" for x in sorted(pf_c["pair"])), "missing":f"C{pf_c['missing']}", "prob":final_prob})
        if not candidates: return None
        return max(candidates, key=lambda x: x["prob"])

    # ── Mensajes y Resolución ─────────────────────────────────────────────────
    def _fmt_pair_display(self, pair: tuple) -> str:
        nums = sorted([p[1:] for p in pair]); return f"{nums[0]} y {nums[1]}"

    def _build_signal_text(self, attempt: int) -> str:
        bet = int(self.current_bet())
        pair_disp = self._fmt_pair_display(self.active_pair)
        if self.active_type == "DOCENA":
            type_str = "docenas"; singular = "docena"
        else:
            type_str = "columnas"; singular = "columna"
            
        return (f"🎰 ENTRADA CONFIRMADA 🎰\n\n"
                f"🎮 Roulette Russian\n"
                f"🎯 Entrar en las {type_str}: {pair_disp}\n"
                f"💰 Balance: {int(self.bankroll)}\n"
                f"💸 Apuesta total: {bet * 2} (por {singular}: {bet})\n"
                f"⚔️ Cubrir el CERO 🟢\n"
                f"🛟 Max: 1 Gales")

    def _send_signal(self, attempt: int):
        # Ya no se eliminan mensajes, solo se envía la nueva entrada
        tg_send(self._build_signal_text(attempt))

    def _resolve(self, number: int, color: str):
        d, c = get_dozen(number), get_column(number)
        is_zero = (number == 0)
        type_str = self.active_type
        val_num = d if type_str == "DOCENA" else c
        current_attempt = MAX_ATTEMPTS - self.attempts_left # 0 para Gale 0, 1 para Gale 1

        if is_zero:
            # EMPATE: Si sale 0 y cubrimos, recuperamos la apuesta. Ganancia 0.
            self.attempts_left -= 1
            tg_send(f"🟠 EMPATE {number} — ZERO — 🔄 GALE #{current_attempt}\n"
                    f"🉑 Para la próxima ganaremos 0.00 🉑\n"
                    f"💰 Balance actual: {self.bankroll:.1f}")
            self.stats.record('EMPATE', current_attempt, 0, 0, type_str, self.bankroll)
            
            if self.attempts_left > 0:
                self._send_signal(current_attempt + 2) # Envía aviso para Gale 1 (intento 2)
            else:
                self._check_stats()
                self._reset_signal()
            return

        won = (type_str == "DOCENA" and d != 0 and f"D{d}" in self.active_pair) or \
              (type_str == "COLUMNA" and c != 0 and f"C{c}" in self.active_pair)

        if won:
            profit = self.current_bet() # En docenas/columnas se gana 1x la apuesta neta
            self.bankroll = round(self.bankroll + profit, 2)
            tg_send(f"✅ WIN {number} — {type_str} {val_num} — 🔄 GALE #{current_attempt}\n"
                    f"🎉 Felicidades has ganado {int(profit)} 🎉\n"
                    f"💰 Balance actual: {self.bankroll:.1f}")
            self.stats.record('WIN', current_attempt, number, val_num, type_str, self.bankroll)
            self._check_stats()
            self._reset_signal()
        else:
            loss_amt = self.current_bet() * 2 # Se pierden las 2 apuestas realizadas
            self.bankroll = round(self.bankroll - loss_amt, 2)
            self.attempts_left -= 1
            
            tg_send(f"❌ LOSS {number} — {type_str} {val_num} — 🔄 GALE #{current_attempt}\n"
                    f"🚨 Para la próxima ganaremos -{int(loss_amt)} 🚨\n"
                    f"💰 Balance actual: {self.bankroll:.1f}")
            self.stats.record('LOSS', current_attempt, number, val_num, type_str, self.bankroll)
            
            if self.attempts_left > 0:
                self._send_signal(current_attempt + 2) # Envía señal para el intento 2 (Gale 1)
            else:
                self._check_stats()
                self._reset_signal()

    def _reset_signal(self): 
        self.signal_active = False; self.active_pair = (); self.attempts_left = MAX_ATTEMPTS

    def process_number(self, number: int):
        try: self._process_inner(number)
        except Exception as e: logger.error(f"Error: {e}", exc_info=True); self._reset_signal()

    def _process_inner(self, number: int):
        color = REAL_COLOR_MAP.get(number, "VERDE")
        d = get_dozen(number); c = get_column(number)
        logger.info(f"[RussianDC] 🎰 #{len(self.spin_history)+1}: {number} {color} | D{d} C{c}")
        
        self._update_state(number)
        
        if not self.warmup_done:
            self.ws_count += 1
            if self.ws_count < WARMUP_SPINS: return
            self.warmup_done = True
            tg_send("🟢 <b>Russian Roulette DC</b> — Sistema PF+PH+ML Listo.")
            logger.info("[RussianDC] ✅ WARMUP COMPLETADO")
            
        if self.signal_active: 
            self._resolve(number, color)
        else:
            sig = self._detect_signal()
            if sig:
                self.signal_active = True; self.active_type = sig["type"]
                self.active_pair = sig["pair"]; self.active_missing = sig["missing"]
                self.attempts_left = MAX_ATTEMPTS; self.trigger_number = number; self.trigger_color = color
                self._send_signal(1) # Intento 1 (Gale 0)
                logger.info(f"[RussianDC] 🎯 SEÑAL {sig['type']}: {sig['pair']} (Prob: {sig['prob']:.0%})")

    def _check_stats(self):
        if not self.stats.should_send(): return
        text = self.stats.get_stats_text(self.bankroll)
        self.stats.mark_sent()
        tg_send(text)

    # ── WebSocket ─────────────────────────────────────────────────────────────
    async def run_ws(self):
        reconnect_delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10) as ws:
                    await ws.send(json.dumps({"type":"subscribe","key":WS_KEY,"casinoId":CASINO_ID}))
                    logger.info(f"[RussianDC] ✅ WS conectado key={WS_KEY}"); reconnect_delay = 5
                    async for raw in ws:
                        try: data = json.loads(raw)
                        except: continue
                        if not isinstance(data, dict): continue
                        results = data.get("last20Results")
                        if results and isinstance(results, list):
                            latest = results[0]; game_id = str(latest.get("gameId",""))
                            if game_id == self.last_game_id: continue
                            self.last_game_id = game_id
                            try: number = int(latest.get("result",""))
                            except: continue
                            if 0 <= number <= 36: self.process_number(number)
                            continue
                        for key in ("result","number","outcome","winningNumber"):
                            if key in data:
                                try:
                                    n = int(data[key])
                                    if 0 <= n <= 36: self.process_number(n)
                                except: pass; break
            except Exception as e:
                logger.warning(f"[RussianDC] WS desconectado: {e}. Recon en {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay); reconnect_delay = min(reconnect_delay*2, 60)

# ─── FLASK & SELF-PING ───────────────────────────────────────────────────────
app = Flask(__name__); engine: Optional[RussianRouletteEngine] = None
@app.route("/")
def home(): return jsonify({"status": "ok", "bot": "Russian DC PF+PH+ML Cross"})
@app.route("/ping")
def ping(): return jsonify({"status":"pong","ts":time.time()})
@app.route("/health")
def health(): return jsonify({"status":"healthy","warmup": engine.warmup_done if engine else False, "spins": len(engine.spin_history) if engine else 0})

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL","").rstrip("/")
    if not url: return
    ping_url = f"{url}/ping"; await asyncio.sleep(30)
    while True:
        try: urllib.request.urlopen(ping_url, timeout=15)
        except: pass
        await asyncio.sleep(240)

@bot.message_handler(commands=['start','help'])
def cmd_start(message):
    bot.reply_to(message, "<b>🎰 Russian DC Bot (PF+PH+ML)</b>\n\nPF: Últimos 5 (65%)\nPH: Histórico últ nº (35%)\nML: Cross(D+C)+Markov+AMX\nUmbral: 80%\n\n/status\n/stats\n/reset", parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    if not engine: return
    st = f"🟢 Señal activa: {engine.active_pair}" if engine.signal_active else "⚪ Idle"
    bot.reply_to(message, f"<b>📊 ESTADO</b>\n\nEstado: {st}\nGiros: {len(engine.spin_history)}\nBankroll: {engine.bankroll:.1f}", parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(message):
    if not engine: return
    text = engine.stats.get_stats_text(engine.bankroll)
    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    if engine: engine.stats = DetailedStats(); engine.bankroll = 100000.0
    bot.reply_to(message,"🔄 <b>Resetado</b>",parse_mode="HTML")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engine
    engine = RussianRouletteEngine()
    tasks = [asyncio.create_task(engine.run_ws()), asyncio.create_task(self_ping_loop())]
    def _poll(): bot.polling(none_stop=True, interval=1, timeout=30)
    threading.Thread(target=_poll, daemon=True).start()
    logger.info("[RussianDC] 🎰 Bot iniciado — Esperando conexión WS...")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot detenido.")
