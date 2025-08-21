#!/usr/bin/env python3
# Rebuild manifest_latest.csv aus dem LetsCast-Feed
# - Stabile, fortlaufende Nummern (001, 002, …) über ALLE Feed-Einträge
# - VTT-Links: raw.githubusercontent + jsDelivr, Feed-VTT nur wenn echt (text/vtt oder "WEBVTT")
# - Commit-Logik macht die Action

import io, math, html, sys, re
from pathlib import Path
from datetime import datetime
import requests, feedparser, pandas as pd
from dateutil import parser as dateparser

FEED_URL = "https://letscast.fm/podcasts/bauertothepeople-b2p-der-podcast-hinter-den-kulissen-von-deinem-essen-de5c15c4/feed"
OUT_CSV  = "manifest_latest.csv"

S = requests.Session()
S.headers.update({"User-Agent":"b2p-manifest-rebuilder/1.0"})

def norm_date(e):
    # versuche ISO-Strings
    for k in ("published","updated","created","date"):
        v = e.get(k)
        if v:
            try: return dateparser.parse(v)
            except: pass
    # versuche struct_time
    for k in ("published_parsed","updated_parsed"):
        v = e.get(k)
        if v:
            try: return datetime(*v[:6])
            except: pass
    return None

def is_noise(title:str)->bool:
    return "hintergrundrauschen" in (title or "").lower()

def podcast_transcript_url(e):
    # 1) links[] mit type text/vtt
    for L in e.get("links", []):
        t = (L.get("type") or "").lower()
        href = L.get("href")
        if href and t.startswith("text/vtt"):
            return href
    # 2) Episodenlink + /transcript.vtt
    link = e.get("link") or ""
    if link:
        return link.rstrip("/") + "/transcript.vtt"
    return None

def has_real_vtt(url:str)->bool:
    try:
        r = S.head(url, allow_redirects=True, timeout=20)
        if r.status_code >= 400:
            return False
        ct = (r.headers.get("Content-Type") or "").lower()
        if "text/vtt" in ct:
            return True
    except Exception:
        pass
    # Fallback: erste Bytes GETten und auf "WEBVTT" prüfen
    try:
        r = S.get(url, stream=True, timeout=30)
        r.raise_for_status()
        first = b""
        for chunk in r.iter_content(chunk_size=2048):
            first += chunk
            if len(first) >= 2048:
                break
        return first.strip().startswith(b"WEBVTT")
    except Exception:
        return False

def main():
    d = feedparser.parse(FEED_URL)
    if d.bozo:
        raise RuntimeError(f"Feed-Fehler: {d.bozo_exception}")
    entries = list(d.entries)
    # Chronologisch ALT → NEU, damit Nummern zu VTT-Dateinamen passen
    entries.sort(key=lambda e: norm_date(e) or datetime.min)

    rows = []
    for i, e in enumerate(entries, start=1):
        num = f"{i:03d}"
        title = e.get("title") or ""
        link  = e.get("link")  or ""
        guid  = e.get("id")    or e.get("guid") or ""
        pub   = norm_date(e)
        it_ep = e.get("itunes_episode")

        vtt_raw = f"https://raw.githubusercontent.com/b2p-hub/b2p-vtts/main/{num}.vtt"
        vtt_cdn = f"https://cdn.jsdelivr.net/gh/b2p-hub/b2p-vtts@main/{num}.vtt"

        vtt_feed = podcast_transcript_url(e)
        vtt_source = "github_raw"
        vtt_final  = vtt_raw

        if vtt_feed and has_real_vtt(vtt_feed):
            vtt_final  = vtt_feed
            vtt_source = "feed"

        rows.append({
            "num": num,
            "title": title,
            "link": link,
            "guid": guid,
            "pub_date": pub.isoformat() if pub else "",
            "itunes_episode": int(it_ep) if isinstance(it_ep,(int,float)) else "",
            "is_noise": is_noise(title),

            "transcript_url_feed": vtt_feed or "",
            "transcript_url_raw":  vtt_raw,
            "transcript_url_jsdelivr": vtt_cdn,
            "transcript_url_final": vtt_final,
            "vtt_source": vtt_source
        })

    df = pd.DataFrame(rows)
    # Sicherheitscheck: keine doppelten num
    if df["num"].duplicated().any():
        dups = df[df["num"].duplicated(keep=False)].sort_values("num")
        raise SystemExit("Duplikate in 'num' entdeckt:\n" + dups[["num","title"]].to_string(index=False))

    df.to_csv(OUT_CSV, index=False)
    print(f"OK: {OUT_CSV} neu erzeugt ({len(df)} Einträge)")

if __name__ == "__main__":
    main()
