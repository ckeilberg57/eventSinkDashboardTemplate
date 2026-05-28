from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, make_response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, func, text, desc
from werkzeug.middleware.proxy_fix import ProxyFix
import os
import json
import re
from collections import Counter, defaultdict
import logging
from logging.handlers import WatchedFileHandler
import requests

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

STRICT_FINALSTATS_FROM_STREAMS_DESTROYED = True

app = Flask(__name__, static_url_path='/pexip-sink/static')
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

DEFAULT_DATA_DIR = "/var/lib/pexip-event-sink"
DATA_DIR = os.getenv("PEXIP_DATA_DIR", DEFAULT_DATA_DIR)
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except PermissionError:
    home_fallback = os.path.expanduser("~/.pexip-event-sink")
    os.makedirs(home_fallback, exist_ok=True)
    DATA_DIR = home_fallback

DB_PATH = os.path.join(DATA_DIR, "pexip_events.db")

app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{DB_PATH}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

LOG_PATH = os.getenv("PEXIP_LOG_PATH", "/var/log/pexip-event-sink.log")
try:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    if not os.path.exists(LOG_PATH):
        open(LOG_PATH, 'a').close()
except PermissionError:
    LOG_PATH = os.path.join(DATA_DIR, "pexip-event-sink.log")
    if not os.path.exists(LOG_PATH):
        open(LOG_PATH, 'a').close()

root = logging.getLogger()
root.setLevel(logging.DEBUG)

for h in list(root.handlers):
    root.removeHandler(h)

file_h = WatchedFileHandler(LOG_PATH)
file_h.setLevel(logging.DEBUG)
file_h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))

console_h = logging.StreamHandler()
console_h.setLevel(logging.INFO)
console_h.setFormatter(logging.Formatter('%(levelname)s %(message)s'))

root.addHandler(file_h)
root.addHandler(console_h)

app.logger.handlers = []
app.logger.propagate = True
app.logger.setLevel(logging.DEBUG)

db = SQLAlchemy(app)


class Event(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_key = db.Column(db.String(200))
    conference_name = db.Column(db.String(255), index=True)
    call_id = db.Column(db.String(100), index=True)
    participant_id = db.Column(db.String(100), index=True)
    vendor = db.Column(db.String(255))
    remote_alias = db.Column(db.String(255), index=True)
    display_name = db.Column(db.String(255))
    remote_alias_key = db.Column(db.String(255), index=True)
    display_name_key = db.Column(db.String(255), index=True)
    disconnect_reason = db.Column(db.String(255))
    connect_time = db.Column(db.Float)
    rx_bandwidth = db.Column(db.Integer)
    tx_bandwidth = db.Column(db.Integer)
    call_quality_now = db.Column(db.String(50))
    packet_loss_details = db.Column(db.Text)
    active = db.Column(db.Boolean, default=True, index=True)
    status = db.Column(db.String(100))
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class FinalStats(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    participant_id = db.Column(db.String(100), index=True)
    conference_name = db.Column(db.String(255), index=True)
    call_id = db.Column(db.String(100), index=True)
    audio_stats = db.Column(db.Text)
    video_stats = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class AmbulanceCall(db.Model):
    __tablename__ = "ambulance_call"
    id = db.Column(db.Integer, primary_key=True)
    tag = db.Column(db.String(64), index=True, nullable=False)
    call_id = db.Column(db.String(100), index=True, nullable=True)
    conference_name = db.Column(db.String(255), nullable=True)
    active = db.Column(db.Boolean, default=True, index=True)
    started_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    ended_at = db.Column(db.DateTime, nullable=True, index=True)


class WorkerVMLoad(db.Model):
    __tablename__ = "worker_vm_load"
    id = db.Column(db.Integer, primary_key=True)
    node_id = db.Column(db.String(64), index=True, nullable=False)
    node_name = db.Column(db.String(128), nullable=False)
    timestamp = db.Column(db.DateTime, index=True, nullable=False, default=datetime.utcnow)
    media_load = db.Column(db.Float, nullable=False)


with app.app_context():
    inspector = inspect(db.engine)

    if 'event' in inspector.get_table_names():
        cols = [col['name'] for col in inspector.get_columns('event')]
        for col, ddl in [
            ('call_id', 'TEXT'),
            ('participant_id', 'TEXT'),
            ('session_key', 'TEXT'),
            ('remote_alias', 'TEXT'),
            ('display_name', 'TEXT'),
            ('remote_alias_key', 'TEXT'),
            ('display_name_key', 'TEXT'),
            ('disconnect_reason', 'TEXT'),
        ]:
            if col not in cols:
                try:
                    with db.engine.connect() as conn:
                        conn.execute(text(f"ALTER TABLE event ADD COLUMN {col} {ddl}"))
                except Exception as e:
                    app.logger.warning(f"Could not add column '{col}': {e}")

    if 'final_stats' in inspector.get_table_names():
        fcols = [col['name'] for col in inspector.get_columns('final_stats')]
        if 'call_id' not in fcols:
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE final_stats ADD COLUMN call_id TEXT"))
            except Exception as e:
                app.logger.warning(f"Could not add column 'FinalStats.call_id': {e}")

    db.create_all()


def _safe_vendor(ua: str) -> str:
    vendor_match = re.search(r'(Chrome|Firefox|Safari|Edge|PexRTC|Poly|Cisco|Webex|Teams|Zoom)', ua or '')
    return vendor_match.group(0) if vendor_match else 'unknown'


def _canonical_alias(s: str) -> str:
    if not s:
        return 'unknown'
    s = re.sub(r'(?i)^sip:', '', s or '')
    s = s.split(';')[0].split('?')[0]
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s or 'unknown'


def _endpoint_key(s: str) -> str:
    if not s:
        return ''
    s = re.sub(r'(?i)^sip:', '', s or '')
    s = s.split(';')[0].split('?')[0]
    s = s.split('@')[0]
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def _extract_remote_alias(info: dict) -> str:
    return (
        info.get('source_alias')
        or info.get('remote_alias')
        or info.get('participant_name')
        or info.get('display_name')
        or info.get('remote_display_name')
        or ''
    )


def _extract_display_name(info: dict) -> str:
    return (
        info.get('display_name')
        or info.get('participant_name')
        or info.get('remote_display_name')
        or info.get('source_alias')
        or info.get('remote_alias')
        or ''
    )


def _latest_alias_for_call(call_id: str) -> str:
    row = (
        Event.query
        .filter(Event.call_id == call_id, Event.conference_name != None)
        .order_by(Event.timestamp.desc())
        .first()
    )
    return (row.conference_name if row and row.conference_name else 'unknown')


def _valid_call_id(col):
    return (col != None) & (col != '') & (col != 'unknown')


def _ended_call_ids_since(since_dt=None):
    q = (
        db.session.query(Event.call_id, func.max(Event.timestamp))
        .filter(
            Event.status == 'conference_ended',
            _valid_call_id(Event.call_id)
        )
    )
    if since_dt:
        q = q.filter(Event.timestamp >= since_dt)
    rows = q.group_by(Event.call_id).all()
    return {r[0] for r in rows}


def _per_call_disconnect_reasons(call_ids):
    if not call_ids:
        return []
    reasons = []
    for cid in call_ids:
        latest_disc = (
            Event.query
            .filter(
                Event.call_id == cid,
                Event.active == False,
                Event.status != None,
                Event.status != '',
                Event.status != 'conference_ended'
            )
            .order_by(Event.timestamp.desc())
            .first()
        )
        reasons.append(latest_disc.status if latest_disc else 'Unknown')
    counts = Counter(reasons)
    return [{"reason": r, "count": c} for r, c in sorted(counts.items(), key=lambda x: (-x[1], x[0]))]


def _extract_quality(info: dict):
    for k in ("call_quality_now", "quality_now", "quality"):
        v = info.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _normalize_packet_loss_to_array(x):
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return [x]
    if isinstance(x, (int, float, str)):
        return [{"value": x}]
    return []


def _extract_packet_loss(info: dict):
    if "packet_loss_history" in info:
        try:
            arr = _normalize_packet_loss_to_array(info["packet_loss_history"])
            return json.dumps(arr)
        except Exception:
            return json.dumps([])
    if "packet_loss" in info:
        try:
            arr = _normalize_packet_loss_to_array(info["packet_loss"])
            return json.dumps(arr)
        except Exception:
            return json.dumps([])
    single = info.get("packet_loss_ms") or info.get("packet_loss_now")
    if single is not None:
        return json.dumps(_normalize_packet_loss_to_array(single))
    return ""


def _resolve_recent_context_for_participant(participant_id: str):
    e = (
        Event.query
        .filter(Event.session_key == participant_id)
        .order_by(Event.timestamp.desc())
        .first()
    )
    alias = (e.conference_name if e and e.conference_name not in (None, '', 'unknown') else None)
    cid   = (e.call_id        if e and e.call_id        not in (None, '', 'unknown') else None)
    return alias, cid


def _strip_jitter_totals(t: dict):
    if isinstance(t, dict):
        t.pop("jitter_avg", None)
    return t


def _merge_or_create_finalstats(participant_id, conf_name, call_id, audio_list, video_list, ts):
    WINDOW_SEC = 30
    q = FinalStats.query.filter(FinalStats.participant_id == participant_id)

    if _valid_call_id(call_id):
        q = q.filter(FinalStats.call_id == call_id)
    else:
        if conf_name and conf_name != 'unknown':
            q = q.filter(FinalStats.conference_name == conf_name)

    q = q.order_by(desc(FinalStats.timestamp))
    fs = q.first()

    if fs and abs((ts - fs.timestamp).total_seconds()) <= WINDOW_SEC:
        try:
            existing_audio = json.loads(fs.audio_stats) if fs.audio_stats else []
        except Exception:
            existing_audio = []
        try:
            existing_video = json.loads(fs.video_stats) if fs.video_stats else []
        except Exception:
            existing_video = []
        if audio_list:
            existing_audio.extend(audio_list)
        if video_list:
            existing_video.extend(video_list)
        fs.audio_stats = json.dumps(existing_audio)
        fs.video_stats = json.dumps(existing_video)
        fs.timestamp = ts
        db.session.add(fs)
    else:
        db.session.add(FinalStats(
            participant_id=participant_id,
            conference_name=conf_name,
            call_id=call_id if _valid_call_id(call_id) else None,
            audio_stats=json.dumps(audio_list or []),
            video_stats=json.dumps(video_list or []),
            timestamp=ts
        ))


@app.route('/pexip-sink/event_sink', methods=['POST'])
def event_sink():
    data = request.get_json(silent=True)
    app.logger.info(f"Received event: {data}")
    if not data:
        return jsonify({"error": "Invalid data"}), 400

    event_type = data.get('event', 'unknown')
    info = data.get('data', {}) or {}

    conf_name_raw = (
        info.get('destination_alias')
        or info.get('conference')
        or info.get('name')
        or 'unknown'
    )
    conf_name = _canonical_alias(conf_name_raw)

    call_id = info.get('conversation_id') or info.get('call_id') or 'unknown'

    related_uuids = info.get('related_uuids')
    participant_id = (
        related_uuids[0]
        if isinstance(related_uuids, list) and related_uuids
        else info.get('uuid', 'unknown')
    )

    vendor = _safe_vendor(info.get('vendor', 'unknown'))
    session_key = f"{participant_id}"
    timestamp = datetime.utcnow()

    remote_alias = _extract_remote_alias(info)
    display_name = _extract_display_name(info)
    remote_alias_key = _endpoint_key(remote_alias)
    display_name_key = _endpoint_key(display_name)

    try:
        if event_type in ['participant_connected', 'participant_updated', 'participant_media_stream_window', 'conference_updated']:
            rx_bandwidth = info.get('rx_bandwidth', 0)
            tx_bandwidth = info.get('tx_bandwidth', 0)
            connect_time = info.get('connect_time', 0)

            if event_type == 'participant_media_stream_window':
                quality_now = info.get('call_quality_now') or _extract_quality(info)
                packet_loss = _extract_packet_loss(info)
            else:
                quality_now = _extract_quality(info)
                packet_loss = _extract_packet_loss(info)

            existing = Event.query.filter_by(session_key=session_key).first()

            if existing and existing.active is False:
                app.logger.info(f"Ignoring late {event_type} for disconnected session {session_key}")
            else:
                if existing:
                    existing.conference_name = conf_name or existing.conference_name
                    if call_id and call_id != 'unknown':
                        existing.call_id = call_id
                    existing.vendor = vendor or existing.vendor
                    if remote_alias:
                        existing.remote_alias = remote_alias
                        existing.remote_alias_key = remote_alias_key
                    if display_name:
                        existing.display_name = display_name
                        existing.display_name_key = display_name_key
                    if connect_time:
                        existing.connect_time = connect_time
                    if isinstance(rx_bandwidth, (int, float)) and rx_bandwidth > 0:
                        existing.rx_bandwidth = int(rx_bandwidth)
                    if isinstance(tx_bandwidth, (int, float)) and tx_bandwidth > 0:
                        existing.tx_bandwidth = int(tx_bandwidth)
                    if quality_now:
                        existing.call_quality_now = quality_now
                    if packet_loss:
                        existing.packet_loss_details = packet_loss
                    existing.active = True
                    existing.timestamp = timestamp
                else:
                    db.session.add(Event(
                        session_key=session_key,
                        conference_name=conf_name,
                        call_id=call_id,
                        participant_id=participant_id,
                        vendor=vendor,
                        remote_alias=remote_alias or None,
                        display_name=display_name or None,
                        remote_alias_key=remote_alias_key or None,
                        display_name_key=display_name_key or None,
                        connect_time=connect_time,
                        rx_bandwidth=int(rx_bandwidth) if isinstance(rx_bandwidth, (int, float)) else None,
                        tx_bandwidth=int(tx_bandwidth) if isinstance(tx_bandwidth, (int, float)) else None,
                        call_quality_now=(quality_now or '0_unknown'),
                        packet_loss_details=packet_loss,
                        active=True,
                        timestamp=timestamp
                    ))

        elif event_type == 'participant_disconnected':
            reason = info.get('disconnect_reason', 'Unknown')

            existing = Event.query.filter_by(session_key=session_key).first()
            if existing:
                existing.active = False
                existing.status = reason
                existing.disconnect_reason = reason
                existing.timestamp = timestamp
                if call_id and (existing.call_id in (None, '', 'unknown')):
                    existing.call_id = call_id
                if conf_name and (existing.conference_name in (None, '', 'unknown')):
                    existing.conference_name = conf_name
                if remote_alias:
                    existing.remote_alias = remote_alias
                    existing.remote_alias_key = remote_alias_key
                if display_name:
                    existing.display_name = display_name
                    existing.display_name_key = display_name_key
            else:
                db.session.add(Event(
                    session_key=session_key,
                    conference_name=conf_name,
                    call_id=call_id,
                    participant_id=participant_id,
                    vendor=vendor,
                    remote_alias=remote_alias or None,
                    display_name=display_name or None,
                    remote_alias_key=remote_alias_key or None,
                    display_name_key=display_name_key or None,
                    active=False,
                    status=reason,
                    disconnect_reason=reason,
                    timestamp=timestamp
                ))

            if (not STRICT_FINALSTATS_FROM_STREAMS_DESTROYED) and isinstance(info.get('media_streams'), list):
                audio = [s for s in info['media_streams'] if s.get('stream_type') == 'audio']
                video = [s for s in info['media_streams'] if s.get('stream_type') == 'video']
                _merge_or_create_finalstats(participant_id, conf_name, call_id, audio, video, timestamp)

            if call_id and call_id != 'unknown':
                db.session.query(Event).filter(
                    (Event.active == True) &
                    ((Event.call_id == None) | (Event.call_id == '') | (Event.call_id == 'unknown')) &
                    (Event.conference_name == conf_name)
                ).update({Event.call_id: call_id}, synchronize_session=False)

        elif event_type in ['conference_started', 'conference_is_started', 'conference_start']:
            tag_raw = (
                info.get('tag')
                or info.get('service_tag')
                or info.get('serviceTag')
                or info.get('conference_tag')
                or info.get('conferenceTag')
                or info.get('destination_alias')
                or info.get('conference')
                or info.get('name')
                or ''
            )

            m = re.search(r'(?i)\bipad\d{3,}\b', str(tag_raw))
            if m:
                ipad_tag = m.group(0)
                ipad_tag = "iPAD" + re.sub(r'(?i)^ipad', '', ipad_tag)

                AmbulanceCall.query.filter_by(tag=ipad_tag, active=True).update(
                    {"active": False, "ended_at": timestamp},
                    synchronize_session=False
                )

                db.session.add(AmbulanceCall(
                    tag=ipad_tag,
                    call_id=(call_id if _valid_call_id(call_id) else None),
                    conference_name=(conf_name if conf_name and conf_name != 'unknown' else None),
                    active=True,
                    started_at=timestamp
                ))

        elif event_type == 'conference_ended':
            name_candidates = [_canonical_alias(x) for x in [
                info.get('name'),
                info.get('conference'),
                info.get('destination_alias'),
                info.get('source_alias')
            ] if x]
            call_candidates = [c for c in [info.get('conversation_id'), info.get('call_id')] if c]

            q = Event.query.filter(Event.active == True)
            if name_candidates and call_candidates:
                q = q.filter((Event.conference_name.in_(name_candidates)) | (Event.call_id.in_(call_candidates)))
            elif name_candidates:
                q = q.filter(Event.conference_name.in_(name_candidates))
            elif call_candidates:
                q = q.filter(Event.call_id.in_(call_candidates))

            q.update(
                {"active": False, "status": "conference_ended", "timestamp": timestamp},
                synchronize_session=False
            )

            try:
                if call_candidates:
                    AmbulanceCall.query.filter(
                        AmbulanceCall.active == True,
                        AmbulanceCall.call_id.in_(call_candidates)
                    ).update({"active": False, "ended_at": timestamp}, synchronize_session=False)

                if name_candidates:
                    AmbulanceCall.query.filter(
                        AmbulanceCall.active == True,
                        AmbulanceCall.conference_name.in_(name_candidates)
                    ).update({"active": False, "ended_at": timestamp}, synchronize_session=False)
            except Exception as e:
                app.logger.warning(f"Could not end AmbulanceCall rows: {e}")

        elif event_type == 'participant_media_streams_destroyed':
            if (conf_name == 'unknown') or (not _valid_call_id(call_id)):
                back_alias, back_cid = _resolve_recent_context_for_participant(participant_id)
                if back_alias:
                    conf_name = back_alias
                if back_cid:
                    call_id = back_cid

            audio, video = [], []
            for stream in info.get('media_streams', []):
                if stream.get('stream_type') == 'audio':
                    audio.append(stream)
                elif stream.get('stream_type') == 'video':
                    video.append(stream)

            _merge_or_create_finalstats(participant_id, conf_name, call_id, audio, video, timestamp)

            try:
                db.session.query(Event).filter(Event.session_key == session_key).update(
                    {"active": False, "status": "conference_ended", "timestamp": timestamp},
                    synchronize_session=False
                )
            except Exception as e:
                app.logger.warning(f"Could not mark participant ended for {session_key}: {e}")

        db.session.commit()
    except Exception as e:
        app.logger.error(f"Error handling event: {e}", exc_info=True)
        db.session.rollback()

    return jsonify({"status": "ok"}), 200


@app.route('/pexip-sink/api/live/<conference_alias>')
def api_live_conference(conference_alias):
    conference_alias = _canonical_alias(conference_alias)

    rows = (
        Event.query
        .filter(Event.conference_name == conference_alias)
        .filter(Event.active == True)
        .order_by(Event.timestamp.desc())
        .all()
    )

    seen = set()
    participants = []

    for row in rows:
        participant_id = row.participant_id or row.session_key
        remote_alias = row.remote_alias or ''
        display_name = row.display_name or row.remote_alias or row.participant_id or ''
        dedupe_key = participant_id or remote_alias or display_name

        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        participants.append({
            'participant_id': participant_id,
            'display_name': display_name,
            'remote_alias': remote_alias,
            'remote_alias_key': row.remote_alias_key or _endpoint_key(remote_alias),
            'display_name_key': row.display_name_key or _endpoint_key(display_name),
            'conference_alias': row.conference_name,
            'status': row.status or 'active',
            'vendor': row.vendor or '',
            'disconnect_reason': row.disconnect_reason or '',
            'timestamp': row.timestamp.isoformat() if row.timestamp else None,
        })

    return jsonify({
        'ok': True,
        'conference_alias': conference_alias,
        'count': len(participants),
        'participants': participants,
    })


@app.route('/pexip-sink/api/live-endpoints')
def api_live_endpoints():
    rows = (
        Event.query
        .filter(Event.active == True)
        .order_by(Event.timestamp.desc())
        .all()
    )

    seen = set()
    items = []

    for row in rows:
        participant_id = row.participant_id or row.session_key
        remote_alias = row.remote_alias or ''
        display_name = row.display_name or row.remote_alias or row.participant_id or ''

        key = row.remote_alias_key or row.display_name_key or participant_id
        if not key or key in seen:
            continue
        seen.add(key)

        items.append({
            'participant_id': participant_id,
            'display_name': display_name,
            'display_name_key': row.display_name_key or _endpoint_key(display_name),
            'remote_alias': remote_alias,
            'remote_alias_key': row.remote_alias_key or _endpoint_key(remote_alias),
            'conference_alias': row.conference_name or '',
            'vendor': row.vendor or '',
            'timestamp': row.timestamp.isoformat() if row.timestamp else None,
        })

    return jsonify({'ok': True, 'items': items})


@app.route("/pexip-sink/admin/cleanup-actives", methods=["POST"])
def admin_cleanup_actives():
    body = request.get_json(silent=True) or {}
    older_than_minutes = int(body.get("older_than_minutes", 60))
    only_unknown = bool(body.get("only_unknown", False))
    mark_ended = bool(body.get("mark_ended", True))

    cutoff = datetime.utcnow() - timedelta(minutes=older_than_minutes)
    q = db.session.query(Event).filter(Event.active == True, Event.timestamp < cutoff)
    if only_unknown:
        q = q.filter(
            (Event.conference_name == None) | (Event.conference_name == '') | (Event.conference_name == 'unknown')
        )

    try:
        if mark_ended:
            updated = q.update(
                {"active": False, "status": "stale_cleanup", "timestamp": datetime.utcnow()},
                synchronize_session=False
            )
            db.session.commit()
            return jsonify({"ok": True, "marked_inactive": updated, "cutoff": cutoff.isoformat()+"Z"}), 200
        else:
            rows = q.all()
            deleted = len(rows)
            for e in rows:
                db.session.delete(e)
            db.session.commit()
            return jsonify({"ok": True, "deleted": deleted, "cutoff": cutoff.isoformat()+"Z"}), 200
    except Exception as e:
        db.session.rollback()
        app.logger.error("/admin/cleanup-actives failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


def _pluck_num(sample, keys):
    for k in keys:
        if k in sample and sample[k] is not None:
            try:
                return float(sample[k])
            except Exception:
                pass
    return None


def _rx_stats_from_sample(s):
    return {
        "sent": _pluck_num(s, ['rx_packets_sent','rx_sent','rx_packets']),
        "recv": _pluck_num(s, ['rx_packets_recv','rx_received']),
        "lost": _pluck_num(s, ['rx_packets_lost','rx_lost']),
        "jitter": _pluck_num(s, ['rx_jitter_ms','rx_jitter','jitter_rx','jitter_ms_rx']),
        "w": _pluck_num(s, ['rx_width','rx_video_width','rx_w']),
        "h": _pluck_num(s, ['rx_height','rx_video_height','rx_h']),
    }


def _tx_stats_from_sample(s):
    return {
        "sent": _pluck_num(s, ['tx_packets_sent','tx_sent','tx_packets']),
        "recv": _pluck_num(s, ['tx_packets_recv','tx_received']),
        "lost": _pluck_num(s, ['tx_packets_lost','tx_lost']),
        "jitter": _pluck_num(s, ['tx_jitter_ms','tx_jitter','jitter_tx','jitter_ms_tx']),
        "w": _pluck_num(s, ['tx_width','tx_video_width','tx_w']),
        "h": _pluck_num(s, ['tx_height','tx_video_height','tx_h']),
    }


def _aggregate_totals(packet_arrays):
    totals = {
        "rx_sent":0.0, "rx_recv":0.0, "rx_lost":0.0, "rx_jitter_sum":0.0, "rx_jitter_n":0,
        "tx_sent":0.0, "tx_recv":0.0, "tx_lost":0.0, "tx_jitter_sum":0.0, "tx_jitter_n":0,
        "last_w":None, "last_h":None
    }
    for arr in packet_arrays:
        if not isinstance(arr, list):
            continue
        for s in arr:
            if not isinstance(s, dict):
                continue
            rx = _rx_stats_from_sample(s)
            tx = _tx_stats_from_sample(s)
            if rx["sent"] is not None: totals["rx_sent"] += rx["sent"]
            if rx["recv"] is not None: totals["rx_recv"] += rx["recv"]
            if rx["lost"] is not None: totals["rx_lost"] += rx["lost"]
            if rx["jitter"] is not None: totals["rx_jitter_sum"] += rx["jitter"]; totals["rx_jitter_n"] += 1
            if tx["sent"] is not None: totals["tx_sent"] += tx["sent"]
            if tx["recv"] is not None: totals["tx_recv"] += tx["recv"]
            if tx["lost"] is not None: totals["tx_lost"] += tx["lost"]
            if tx["jitter"] is not None: totals["tx_jitter_sum"] += tx["jitter"]; totals["tx_jitter_n"] += 1
            w = rx["w"] or tx["w"]; h = rx["h"] or tx["h"]
            if w and h:
                totals["last_w"] = int(w)
                totals["last_h"] = int(h)
    rx_loss_pct = round((totals["rx_lost"] / totals["rx_recv"]) * 100) if totals["rx_recv"] > 0 else None
    tx_loss_pct = round((totals["tx_lost"] / totals["tx_sent"]) * 100) if totals["tx_sent"] > 0 else None
    rx_jitter_avg = round(totals["rx_jitter_sum"] / totals["rx_jitter_n"], 2) if totals["rx_jitter_n"] else None
    tx_jitter_avg = round(totals["tx_jitter_sum"] / totals["tx_jitter_n"], 2) if totals["tx_jitter_n"] else None
    return {
        "rx_sent": int(totals["rx_sent"]),
        "rx_recv": int(totals["rx_recv"]),
        "rx_lost": int(totals["rx_lost"]),
        "rx_loss_pct": rx_loss_pct,
        "rx_jitter_avg": rx_jitter_avg,
        "tx_sent": int(totals["tx_sent"]),
        "tx_recv": int(totals["tx_recv"]),
        "tx_lost": int(totals["tx_lost"]),
        "tx_loss_pct": tx_loss_pct,
        "tx_jitter_avg": tx_jitter_avg,
        "final_w": totals["last_w"],
        "final_h": totals["last_h"],
    }


def _parse_res_xy(s):
    if not s or 'x' not in s:
        return (None, None)
    try:
        w, h = s.lower().split('x', 1)
        return int(float(w)), int(float(h))
    except Exception:
        return (None, None)


def _agg_from_finalstreams(audio_list, video_list):
    rx_recv = rx_lost = tx_sent = tx_lost = 0.0
    start_ts = end_ts = None
    last_rx_res = last_tx_res = None

    def upd_ts(s):
        nonlocal start_ts, end_ts
        st = s.get('start_time'); et = s.get('end_time')
        if isinstance(st, (int, float)):
            start_ts = st if start_ts is None else min(start_ts, st)
        if isinstance(et, (int, float)):
            end_ts = et if end_ts is None else max(end_ts, et)

    for s in (audio_list or []):
        upd_ts(s)
        rx_recv += float(s.get('rx_packets_received', 0) or 0)
        rx_lost += float(s.get('rx_packets_lost', 0) or 0)
        tx_sent += float(s.get('tx_packets_sent', 0) or 0)
        tx_lost += float(s.get('tx_packets_lost', 0) or 0)
        last_rx_res = s.get('rx_resolution') or last_rx_res
        last_tx_res = s.get('tx_resolution') or last_tx_res

    for s in (video_list or []):
        upd_ts(s)
        rx_recv += float(s.get('rx_packets_received', 0) or 0)
        rx_lost += float(s.get('rx_packets_lost', 0) or 0)
        tx_sent += float(s.get('tx_packets_sent', 0) or 0)
        tx_lost += float(s.get('tx_packets_lost', 0) or 0)
        last_rx_res = s.get('rx_resolution') or last_rx_res
        last_tx_res = s.get('tx_resolution') or last_tx_res

    rx_loss_pct = round((rx_lost / rx_recv) * 100) if rx_recv > 0 else None
    tx_loss_pct = round((tx_lost / tx_sent) * 100) if tx_sent > 0 else None

    w, h = None, None
    for r in (last_rx_res, last_tx_res):
        w, h = _parse_res_xy(r)
        if w and h:
            break

    return {
        "rx_totals": {"sent": int(rx_recv + rx_lost), "recv": int(rx_recv), "lost": int(rx_lost), "loss_pct": rx_loss_pct, "jitter_avg": None},
        "tx_totals": {"sent": int(tx_sent), "recv": None, "lost": int(tx_lost), "loss_pct": tx_loss_pct, "jitter_avg": None},
        "final_resolution": {"w": w, "h": h},
        "start_time": start_ts,
        "end_time": end_ts,
    }


def _fetch_merge_finalstats_for_participant(participant_id, call_id=None, alias=None):
    q = FinalStats.query.filter(FinalStats.participant_id == participant_id)
    if _valid_call_id(call_id):
        q = q.filter(FinalStats.call_id == call_id)
    elif alias:
        q = q.filter(FinalStats.conference_name == alias)
    q = q.order_by(desc(FinalStats.timestamp))

    rows = q.limit(5).all()
    if not rows:
        return [], []

    newest_ts = rows[0].timestamp
    WINDOW_SEC = 30
    audio_all, video_all = [], []
    for fs in rows:
        if abs((newest_ts - fs.timestamp).total_seconds()) <= WINDOW_SEC:
            try:
                if fs.audio_stats:
                    audio_all.extend(json.loads(fs.audio_stats))
            except Exception:
                pass
            try:
                if fs.video_stats:
                    video_all.extend(json.loads(fs.video_stats))
            except Exception:
                pass
    return audio_all, video_all


def _fold_active_events(events):
    flat = []
    grouped = {}

    for e in events:
        alias = e.conference_name or "unknown"
        call_key = e.call_id if e.call_id and e.call_id != 'unknown' else None
        group_key = call_key or alias

        try:
            pl = json.loads(e.packet_loss_details) if e.packet_loss_details else []
            if not isinstance(pl, list):
                pl = []
        except Exception:
            pl = []

        p = {
            "participant_id": e.participant_id or "unknown",
            "display_name": e.display_name or "",
            "remote_alias": e.remote_alias or "",
            "quality_now": e.call_quality_now if e.call_quality_now and e.call_quality_now != "0_unknown" else "0_unknown",
            "rx_bandwidth": e.rx_bandwidth if e.rx_bandwidth and e.rx_bandwidth > 0 else None,
            "tx_bandwidth": e.tx_bandwidth if e.tx_bandwidth and e.tx_bandwidth > 0 else None,
            "packet_loss": pl,
            "connect_time": e.connect_time if e.connect_time is not None else None,
            "_ts": int(e.timestamp.timestamp())
        }
        flat.append({
            "session_key": e.session_key,
            "conference": alias,
            "call_id": e.call_id or "unknown",
            "vendor": e.vendor or "unknown",
            "status": "active",
            **p
        })

        if group_key not in grouped:
            grouped[group_key] = {
                "group": group_key,
                "conference": alias,
                "call_id": e.call_id or "unknown",
                "vendor": e.vendor or "unknown",
                "participants": [],
                "status": "active",
                "_ts": int(e.timestamp.timestamp())
            }

        g = grouped[group_key]
        g["_ts"] = max(g["_ts"], int(e.timestamp.timestamp()))
        if e.vendor and g["vendor"] == "unknown":
            g["vendor"] = e.vendor
        if p["participant_id"] not in [x["participant_id"] for x in g["participants"]]:
            g["participants"].append(p)

    grouped_list = []
    for g in grouped.values():
        parts = g["participants"]
        if not parts:
            continue
        all_unknown = all(
            (pp["participant_id"] == "unknown" and
             not pp["rx_bandwidth"] and not pp["tx_bandwidth"] and
             (pp["quality_now"] == "0_unknown"))
            for pp in parts
        )
        if all_unknown:
            continue
        grouped_list.append(g)

    grouped_list.sort(key=lambda x: x["_ts"])
    return flat, grouped_list


@app.route('/pexip-sink/chart-data')
def chart_data():
    now = datetime.utcnow()

    active_events = Event.query.filter(Event.active == True).order_by(Event.timestamp.asc()).all()
    flat_active, grouped_active = _fold_active_events(active_events)

    since = now - timedelta(hours=24)
    recent_calls = [r[0] for r in db.session.query(Event.call_id).filter(_valid_call_id(Event.call_id), Event.timestamp >= since).distinct().all()]
    active_call_ids = {r[0] for r in db.session.query(Event.call_id).filter(_valid_call_id(Event.call_id), Event.active == True).distinct().all()}
    ended_candidates = [cid for cid in recent_calls if cid not in active_call_ids]

    call_end_ts = {}
    for cid in ended_candidates:
        row = db.session.query(func.max(Event.timestamp)).filter(Event.call_id == cid).first()
        if row and row[0]:
            call_end_ts[cid] = int(row[0].timestamp())

    ended_signature = f"{len(call_end_ts)}-{max(call_end_ts.values()) if call_end_ts else 0}"

    ENDED_LIMIT = int(os.getenv("PEXIP_ENDED_LIMIT", "20"))
    top_calls = [cid for cid, _ in sorted(call_end_ts.items(), key=lambda kv: kv[1], reverse=True)[:ENDED_LIMIT]]

    ended_participants = []
    for call_id in top_calls:
        display_alias = _latest_alias_for_call(call_id)
        rows = Event.query.filter(Event.call_id == call_id).order_by(Event.timestamp.desc()).all()
        end_ts = call_end_ts.get(call_id)

        by_pid = defaultdict(list)
        for e in rows:
            pid = e.participant_id or "unknown"
            by_pid[pid].append(e)

        for pid, plist in by_pid.items():
            audio, video = _fetch_merge_finalstats_for_participant(pid, call_id=call_id, alias=display_alias)

            per_stream = [
                ("audio", audio, []),
                ("video", [], video),
            ]

            any_emitted = False
            for stype, a_list, v_list in per_stream:
                if not a_list and not v_list:
                    continue

                rx_tx = _agg_from_finalstreams(a_list, v_list)
                rx_tot = _strip_jitter_totals(rx_tx["rx_totals"])
                tx_tot = _strip_jitter_totals(rx_tx["tx_totals"])
                final_w = rx_tx["final_resolution"]["w"]
                final_h = rx_tx["final_resolution"]["h"]
                start_time = rx_tx["start_time"]

                quality_final = None
                vendor = None
                for e in plist:
                    vendor = vendor or e.vendor
                    if e.call_quality_now and e.call_quality_now != '0_unknown' and not quality_final:
                        quality_final = e.call_quality_now

                ended_participants.append({
                    "session_key": plist[0].session_key if plist else "",
                    "conference": display_alias,
                    "call_id": call_id,
                    "vendor": vendor or "unknown",
                    "participant_id": pid,
                    "quality_now": quality_final or "0_unknown",
                    "rx_totals": rx_tot,
                    "tx_totals": tx_tot,
                    "final_resolution": {"w": final_w, "h": final_h},
                    "connect_time": start_time if start_time is not None else None,
                    "ended_at": end_ts,
                    "status": "conference_ended",
                    "stream_type": stype,
                    "_ts": end_ts or (int(plist[0].timestamp.timestamp()) if plist else int(now.timestamp()))
                })
                any_emitted = True

            if not any_emitted:
                rx_tot = _strip_jitter_totals({"sent": None, "recv": None, "lost": None, "loss_pct": None, "jitter_avg": None})
                tx_tot = _strip_jitter_totals({"sent": None, "recv": None, "lost": None, "loss_pct": None, "jitter_avg": None})
                ended_participants.append({
                    "session_key": plist[0].session_key if plist else "",
                    "conference": display_alias,
                    "call_id": call_id,
                    "vendor": (plist[0].vendor if plist and plist[0].vendor else "unknown"),
                    "participant_id": pid,
                    "quality_now": "0_unknown",
                    "rx_totals": rx_tot,
                    "tx_totals": tx_tot,
                    "final_resolution": {"w": None, "h": None},
                    "connect_time": None,
                    "ended_at": end_ts,
                    "status": "conference_ended",
                    "stream_type": "unknown",
                    "_ts": end_ts or (int(plist[0].timestamp.timestamp()) if plist else int(now.timestamp()))
                })

    total_calls = (
        db.session.query(Event.call_id)
        .filter(Event.timestamp >= now - timedelta(days=7), _valid_call_id(Event.call_id))
        .distinct()
        .count()
    )
    recent_consults = (
        db.session.query(Event.call_id)
        .filter(Event.timestamp >= now - timedelta(hours=1), _valid_call_id(Event.call_id))
        .distinct()
        .count()
    )
    disconnect_reasons = _per_call_disconnect_reasons(set(call_end_ts.keys()))

    for ep in ended_participants:
        ep['rx_totals'] = _strip_jitter_totals(ep.get('rx_totals', {}))
        ep['tx_totals'] = _strip_jitter_totals(ep.get('tx_totals', {}))

    active_ambulance = (
        AmbulanceCall.query
        .filter(AmbulanceCall.active == True)
        .order_by(AmbulanceCall.started_at.desc())
        .first()
    )

    ambulance_payload = {
        "active": True,
        "tag": active_ambulance.tag,
        "started_at": int(active_ambulance.started_at.timestamp())
    } if active_ambulance else {"active": False}

    return jsonify({
        "participants": flat_active,
        "participants_grouped": grouped_active,
        "ended_participants": ended_participants,
        "wrapups": [],
        "total_calls": total_calls,
        "recent_consults": recent_consults,
        "disconnect_reasons": disconnect_reasons,
        "ended_signature": ended_signature,
        "ambulance": ambulance_payload
    })


def _no_cache_json(payload, status=200):
    resp = make_response(jsonify(payload), status)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


def _floor_15m(dt):
    m = (dt.minute // 15) * 15
    return dt.replace(minute=m, second=0, microsecond=0)


def _decimal_quarter_label(dt):
    if dt.minute == 0:
        return str(dt.hour)
    return f".{dt.minute:02d}"


RETENTION_HOURS  = int(os.getenv("PEXIP_VMLOAD_RETENTION_HOURS", "4"))
BUCKET_MINUTES   = int(os.getenv("PEXIP_VMLOAD_BUCKET_MINUTES",  "15"))


@app.route("/pexip-sink/worker-vm/status")
def worker_vm_status():
    mgmt_url  = os.getenv("PEXIP_MGMT_URL", "https://cklab-pexmgr.ck-collab-engtest.com/api/admin/status/v1/worker_vm/")
    mgmt_user = os.getenv("PEXIP_MGMT_USER", "admin")
    mgmt_pass = os.getenv("PEXIP_MGMT_PASS", "")

    try:
        r = requests.get(mgmt_url, auth=(mgmt_user, mgmt_pass), timeout=10, verify=False)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        return jsonify({"error": f"Mgmt fetch failed: {e}"}), 502

    raw = payload
    candidates = [
        raw.get("results") if isinstance(raw, dict) else None,
        raw.get("objects") if isinstance(raw, dict) else None,
        raw.get("workers") if isinstance(raw, dict) else None,
        raw.get("items")   if isinstance(raw, dict) else None,
    ]
    nodes = next((x for x in candidates if isinstance(x, list)), None)
    if nodes is None and isinstance(raw, list):
        nodes = raw
    nodes = nodes or []

    def norm(v):
        try:
            x = float(v or 0.0)
        except Exception:
            x = 0.0
        if x >= 1.0:
            x = x / 100.0
        if x < 0.0:
            x = 0.0
        if x > 1.0:
            x = 1.0
        return x

    def getint(d, *keys):
        for k in keys:
            if k in d and d[k] is not None:
                try:
                    return int(float(d[k]))
                except Exception:
                    pass
        return 0

    now_utc = datetime.utcnow()
    cutoff  = now_utc - timedelta(hours=RETENTION_HOURS)

    try:
        db.session.query(WorkerVMLoad).filter(WorkerVMLoad.timestamp < cutoff).delete()
        for n in nodes:
            db.session.add(WorkerVMLoad(
                node_id=str(n.get("id","")),
                node_name=(n.get("name","") or str(n.get("id",""))),
                timestamp=now_utc,
                media_load=norm(n.get("media_load")),
            ))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"DB write failed: {e}"}), 500

    start_tick = _floor_15m(cutoff)
    end_tick   = _floor_15m(now_utc)
    step       = timedelta(minutes=BUCKET_MINUTES)

    hour_labels_iso = []
    hour_labels_display = []
    t = start_tick
    while t <= end_tick:
        hour_labels_iso.append(t.strftime("%Y-%m-%d %H:%M:00"))
        hour_labels_display.append(_decimal_quarter_label(t))
        t += step

    rows = (
        db.session.query(
            WorkerVMLoad.node_id, WorkerVMLoad.node_name,
            WorkerVMLoad.timestamp, WorkerVMLoad.media_load
        )
        .filter(WorkerVMLoad.timestamp >= cutoff)
        .order_by(WorkerVMLoad.node_id, WorkerVMLoad.timestamp)
        .all()
    )

    def bucket_label(dt):
        return _floor_15m(dt).strftime("%Y-%m-%d %H:%M:00")

    by_node_points = {}
    by_node_buckets = {}
    for node_id, node_name, ts, media in rows:
        by_node_points.setdefault(node_id, {"name": node_name, "points": []})["points"].append(
            {"t": ts.isoformat()+"Z", "v": round(float(media), 4)}
        )
        lbl = bucket_label(ts)
        by_node_buckets.setdefault(node_id, {"name": node_name, "buckets": {}})["buckets"].setdefault(lbl, []).append(float(media))

    response_nodes = []
    for n in nodes:
        node_id   = str(n.get("id",""))
        node_name = n.get("name","") or node_id
        pts = by_node_points.get(node_id, {"points": []})["points"]
        media_now = pts[-1]["v"] if pts else norm(n.get("media_load"))

        bmap = by_node_buckets.get(node_id, {"buckets": {}})["buckets"]
        series = []
        for lbl in hour_labels_iso:
            vals = bmap.get(lbl)
            avg = (sum(vals) / len(vals)) if vals else 0.0
            series.append({"hour": lbl, "avg_media": round(avg, 4)})

        response_nodes.append({
            "id": node_id,
            "name": node_name,
            "system_location": n.get("system_location",""),
            "version": n.get("version",""),
            "node_type": n.get("node_type",""),
            "max_full_hd_calls": getint(n, "max_full_hd_calls","max_full_hd","max_full_hd_capacity"),
            "max_sd_calls": getint(n, "max_sd_calls","max_sd","max_sd_capacity"),
            "max_audio_calls": getint(n, "max_audio_calls","max_audio","max_audio_capacity"),
            "media_load_now": media_now,
            "points": pts,
            "media_load_series": series,
        })

    totals = {
        "full_hd": sum(n["max_full_hd_calls"] for n in response_nodes),
        "sd": sum(n["max_sd_calls"] for n in response_nodes),
        "audio": sum(n["max_audio_calls"] for n in response_nodes),
    }

    return _no_cache_json({
        "generated_at": now_utc.isoformat()+"Z",
        "polled_at": now_utc.isoformat()+"Z",
        "hour_labels": hour_labels_iso,
        "hour_labels_display": hour_labels_display,
        "nodes": response_nodes,
        "totals": totals
    })


@app.route("/pexip-sink/licensing/status")
def licensing_status():
    lic_url = os.getenv(
        "PEXIP_MGMT_LICENSE_URL",
        "https://cklab-pexmgr.ck-collab-engtest.com/api/admin/status/v1/licensing/"
    )
    mgmt_user = os.getenv("PEXIP_MGMT_USER", "admin")
    mgmt_pass = os.getenv("PEXIP_MGMT_PASS", "")

    try:
        r = requests.get(lic_url, auth=(mgmt_user, mgmt_pass), timeout=10, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return jsonify({"error": f"Licensing fetch failed: {e}"}), 502

    obj = None
    if isinstance(data, dict) and isinstance(data.get("objects"), list) and data["objects"]:
        obj = data["objects"][0]
    elif isinstance(data, list) and data:
        obj = data[0]

    obj = obj or {}
    items = []
    for k, v in obj.items():
        if not k.endswith("_count"):
            continue
        base = k[:-6]
        total_key = f"{base}_total"
        count = int(obj.get(k, 0) or 0)
        total = int(obj.get(total_key, 0) or 0)
        items.append({
            "key": base,
            "label": base.replace("_", " ").upper(),
            "count": count,
            "total": total
        })

    return jsonify({
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "items": items
    })


# -----------------------------------------------------------------------------
# Infinity policy toggle controls
# -----------------------------------------------------------------------------
POLICY_NORMAL = "/api/admin/configuration/v1/policy_server/8/"
POLICY_TEAMS_DR = "/api/admin/configuration/v1/policy_server/19/"

POLICY_LABELS = {
    POLICY_NORMAL: "Normal / Teams active",
    POLICY_TEAMS_DR: "Teams DR active",
}

# Location IDs are intentionally not configured. The dashboard stores only names
# and resolves those names to Pexip resource URIs dynamically. This makes the
# package portable across Infinity deployments where numeric location IDs differ.
DEFAULT_POLICY_LOCATION_NAMES = "NC-LOC,NC-EDGE-LOC"


def _policy_target_location_names():
    raw = os.getenv("PEXIP_POLICY_LOCATION_NAMES", DEFAULT_POLICY_LOCATION_NAMES)
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def _policy_auth():
    return (
        os.getenv("PEXIP_MGMT_USER", "admin"),
        os.getenv("PEXIP_MGMT_PASS", ""),
    )


def _policy_verify_tls():
    return os.getenv("PEXIP_MGMT_VERIFY_TLS", "false").lower() in ("1", "true", "yes", "on")


def _policy_base_url():
    return os.getenv("PEXIP_MGMT_BASE_URL", "https://cklab-pexmgr.ck-collab-engtest.com").rstrip("/")


def _absolute_mgmt_url(resource_uri_or_url):
    value = str(resource_uri_or_url or "")
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("/"):
        return f"{_policy_base_url()}{value}"
    return f"{_policy_base_url()}/{value}"


def _policy_id(policy_server):
    m = re.search(r"/policy_server/(\d+)/", str(policy_server or ""))
    return m.group(1) if m else "unknown"


def _policy_label(policy_server):
    policy_id = _policy_id(policy_server)
    if policy_id == _policy_id(POLICY_NORMAL):
        return POLICY_LABELS[POLICY_NORMAL]
    if policy_id == _policy_id(POLICY_TEAMS_DR):
        return POLICY_LABELS[POLICY_TEAMS_DR]
    return f"Policy {policy_id}"


def _fetch_all_system_locations():
    """Return all Pexip system locations from the Management API."""
    auth = _policy_auth()
    verify_tls = _policy_verify_tls()
    url = f"{_policy_base_url()}/api/admin/configuration/v1/system_location/"
    locations = []

    # Pexip endpoints normally return all locations in one page, but follow next
    # if present so this keeps working in larger deployments.
    for _ in range(10):
        r = requests.get(url, auth=auth, timeout=15, verify=verify_tls)
        r.raise_for_status()
        data = r.json()
        batch = data.get("objects", data if isinstance(data, list) else [])
        if isinstance(batch, list):
            locations.extend(batch)
        next_url = (data.get("meta") or {}).get("next") if isinstance(data, dict) else None
        if not next_url:
            break
        url = _absolute_mgmt_url(next_url)
    return locations


def _target_policy_locations():
    target_names = _policy_target_location_names()
    all_locations = _fetch_all_system_locations()
    selected = []
    ignored = []
    found_names = set()

    for data in all_locations:
        loc_name = (data.get("name") or "").strip()
        loc_key = loc_name.lower()
        if target_names and loc_key not in target_names:
            ignored.append({
                "name": loc_name or "Unnamed location",
                "url": _absolute_mgmt_url(data.get("resource_uri", "")),
            })
            continue

        found_names.add(loc_key)
        selected.append(data)

    missing = sorted([name for name in target_names if name not in found_names])
    return selected, ignored, missing


def _location_status_from_data(data):
    loc_name = data.get("name") or "Unnamed location"
    resource_uri = data.get("resource_uri") or f"/api/admin/configuration/v1/system_location/{data.get('id')}/"
    url = _absolute_mgmt_url(resource_uri)
    policy = data.get("policy_server", "")
    return {
        "name": loc_name,
        "url": url,
        "policy_server": policy,
        "policy_id": _policy_id(policy),
        "policy_label": _policy_label(policy),
        "ok": True,
    }


def _get_infinity_policy_status():
    target_names = _policy_target_location_names()
    locations = []
    ignored_locations = []
    missing_locations = []

    try:
        selected, ignored_locations, missing_locations = _target_policy_locations()
        locations = [_location_status_from_data(data) for data in selected]
    except Exception as e:
        locations.append({
            "name": "Pexip Management API",
            "url": f"{_policy_base_url()}/api/admin/configuration/v1/system_location/",
            "policy_server": "",
            "policy_id": "error",
            "policy_label": "Unable to read policy",
            "ok": False,
            "error": str(e),
        })

    for missing in missing_locations:
        locations.append({
            "name": missing,
            "url": "",
            "policy_server": "",
            "policy_id": "missing",
            "policy_label": "Configured location name was not found",
            "ok": False,
            "error": "Location name was not returned by the Pexip Management API.",
        })

    policy_ids = {x["policy_id"] for x in locations if x.get("ok")}
    mixed = len(policy_ids) > 1
    current_policy_id = next(iter(policy_ids)) if len(policy_ids) == 1 else "mixed"

    normal_id = _policy_id(POLICY_NORMAL)
    teams_dr_id = _policy_id(POLICY_TEAMS_DR)

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "current_policy_id": current_policy_id,
        "mixed": mixed,
        "teams_dr_active": current_policy_id == teams_dr_id and not mixed,
        "normal_active": current_policy_id == normal_id and not mixed,
        "target_location_names": sorted(target_names),
        "locations": locations,
        "ignored_locations": ignored_locations,
        "missing_locations": missing_locations,
    }


@app.route("/pexip-sink/api/infinity-policy/status")
def infinity_policy_status():
    return jsonify(_get_infinity_policy_status())


@app.route("/pexip-sink/api/infinity-policy/locations")
def infinity_policy_locations():
    try:
        items = []
        for loc in _fetch_all_system_locations():
            items.append({
                "id": loc.get("id"),
                "name": loc.get("name"),
                "resource_uri": loc.get("resource_uri"),
                "selected": (loc.get("name") or "").strip().lower() in _policy_target_location_names(),
                "policy_server": loc.get("policy_server"),
                "policy_id": _policy_id(loc.get("policy_server")),
                "policy_label": _policy_label(loc.get("policy_server")),
            })
        return jsonify({"ok": True, "locations": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502


@app.route("/pexip-sink/api/infinity-policy/toggle", methods=["POST"])
def infinity_policy_toggle():
    payload = request.get_json(silent=True) or {}
    mode = payload.get("mode")

    if mode != "teams_dr":
        return jsonify({"error": "mode must be 'teams_dr'"}), 400

    current = _get_infinity_policy_status()
    target_policy = POLICY_NORMAL if current.get("teams_dr_active") else POLICY_TEAMS_DR

    auth = _policy_auth()
    verify_tls = _policy_verify_tls()
    results = []
    all_ok = True

    # Patch only the configured location names that were resolved successfully.
    for loc in current.get("locations", []):
        if not loc.get("ok"):
            continue
        url = loc.get("url")
        name = loc.get("name") or url.rstrip("/").split("/")[-1]
        try:
            r = requests.patch(
                url,
                auth=auth,
                timeout=15,
                verify=verify_tls,
                json={"policy_server": target_policy},
            )
            ok = r.status_code == 202
            all_ok = all_ok and ok
            results.append({
                "name": name,
                "url": url,
                "ok": ok,
                "status_code": r.status_code,
                "response": r.text[:500],
            })
        except Exception as e:
            all_ok = False
            results.append({
                "name": name,
                "url": url,
                "ok": False,
                "error": str(e),
            })

    refreshed = _get_infinity_policy_status()
    return jsonify({
        "ok": all_ok,
        "target_policy": target_policy,
        "target_policy_id": _policy_id(target_policy),
        "target_policy_label": _policy_label(target_policy),
        "results": results,
        "status": refreshed,
    }), 200 if all_ok else 502


@app.route('/pexip-sink/health')
def health():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat() + "Z"}), 200


@app.route('/pexip-sink/')
def dashboard():
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    hour_ago = now - timedelta(hours=1)

    total_calls = (
        db.session.query(Event.call_id)
        .filter(Event.timestamp >= week_ago, _valid_call_id(Event.call_id))
        .distinct()
        .count()
    )
    consultations_last_hour = (
        db.session.query(Event.call_id)
        .filter(Event.timestamp >= hour_ago, _valid_call_id(Event.call_id))
        .distinct()
        .count()
    )

    ended_call_ids = _ended_call_ids_since(None)
    disconnects = _per_call_disconnect_reasons(ended_call_ids)

    return render_template(
        "dashboard.html",
        total_calls=total_calls,
        consultations_last_hour=consultations_last_hour,
        disconnect_reasons=[(d["reason"], d["count"]) for d in disconnects]
    )


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5050, debug=False)