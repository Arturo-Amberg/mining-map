"""
inject_reserves.py
Cruza depósitos de cobre del GDB del SERNAGEOMIN con las faenas del mapa web
e inyecta los datos de recursos/reservas en index.html.

Datos que agrega a RAW:
  RAW.reserves_by_mk  — keyed por match_key (para columna de pronóstico)
  RAW.reserves_by_fid — keyed por id_faena  (para popup de instalaciones)
"""

import json, re, pyogrio
import numpy as np
from pathlib import Path

# ── Rutas ────────────────────────────────────────────────────────────────────
HTML   = Path(__file__).parent / "index.html"
GDB    = Path(__file__).parent.parent / "Estudio" / "EMC_SNGM.gdb"
DIST_KM_THRESH = 20   # máx km para asignar un depósito a una faena

# ── Leer GDB ────────────────────────────────────────────────────────────────
gdf = pyogrio.read_dataframe(str(GDB), layer="BD_EMC_MET_IND")
cobre = gdf[
    gdf[["CRITICO_1","CRITICO_2","CRITICO_3","CRITICO_4"]]
    .apply(lambda row: row.str.contains("Cobre", na=False).any(), axis=1)
].dropna(subset=["LATITUD","LONGITUD"]).copy()

def cu_fields(row):
    """Extrae campos de Cu según en qué posición CRITICO_N está Cobre."""
    for n in [1,2,3,4]:
        if row.get(f"CRITICO_{n}") == "Cobre":
            return {
                "cu_rec_mt":  row.get(f"RECURSOS_TOTALES_{n}_TONELAJE_MT"),
                "cu_rec_ley": row.get(f"RECURSOS_TOTALES_{n}_LEY_PCT"),
                "cu_rec_t":   row.get(f"CONTENIDO_METALICO_RECURSO_{n}_T"),
                "cu_res_mt":  row.get(f"RESERVAS_TOTALES_{n}_TONELAJE_MT"),
                "cu_res_ley": row.get(f"RESERVAS_TOTALES_{n}_LEY_PCT"),
                "cu_res_t":   row.get(f"CONTENIDO_METALICO_RESERVA_{n}_T"),
            }
    return {}

def clean(v):
    """None/NaN → None, float → rounded."""
    if v is None: return None
    try:
        f = float(v)
        return None if np.isnan(f) else round(f, 4)
    except Exception:
        return None

# ── Leer HTML y extraer RAW ──────────────────────────────────────────────────
html = HTML.read_text(encoding="utf-8")

raw_start_tag = "const RAW = "
idx   = html.index(raw_start_tag) + len(raw_start_tag)
brace = 0
end   = idx
for i, c in enumerate(html[idx:], idx):
    if c == "{": brace += 1
    elif c == "}": brace -= 1
    if brace == 0: end = i + 1; break

raw = json.loads(html[idx:end])
faenas = raw["faenas"]
fa_coords = np.array([[f["lat"], f["lon"]] for f in faenas])

# ── Construir lookups ────────────────────────────────────────────────────────
reserves_by_fid = {}   # id_faena  → deposit dict
reserves_by_mk  = {}   # match_key → deposit dict (solo faenas con producción)

# Para cada depósito de cobre, encontrar la faena más cercana
for _, row in cobre.iterrows():
    dep_coord = np.array([row["LATITUD"], row["LONGITUD"]])
    dists_km  = np.linalg.norm(fa_coords - dep_coord, axis=1) * 111.0
    i_min     = int(np.argmin(dists_km))
    dist      = float(dists_km[i_min])

    if dist > DIST_KM_THRESH:
        continue

    faena   = faenas[i_min]
    fid     = faena["id_faena"]
    mk      = faena.get("match_key") or ""
    cu      = cu_fields(row)

    depo = {
        "deposito":  row["NOMBRE_DEPOSITO"],
        "estado":    row["ESTADO_DEPOSITO"],
        "modelo":    row.get("MODELO_DEPOSITO"),
        "minerales": " · ".join(filter(None, [
            row.get("CRITICO_1"), row.get("CRITICO_2"),
            row.get("CRITICO_3"), row.get("CRITICO_4"),
        ])),
        "cu_rec_mt":  clean(cu.get("cu_rec_mt")),
        "cu_rec_ley": clean(cu.get("cu_rec_ley")),
        "cu_rec_t":   clean(cu.get("cu_rec_t")),
        "cu_res_mt":  clean(cu.get("cu_res_mt")),
        "cu_res_ley": clean(cu.get("cu_res_ley")),
        "cu_res_t":   clean(cu.get("cu_res_t")),
        "referencia": row.get("REFERENCIA"),
        "dist_km":    round(dist, 1),
        "lat":        float(row["LATITUD"]),
        "lon":        float(row["LONGITUD"]),
    }

    # Si ya hay una entrada para esta faena, preferir la de mayor recurso
    def cu_rec(d): return d.get("cu_rec_t") or 0

    if fid not in reserves_by_fid or cu_rec(depo) > cu_rec(reserves_by_fid[fid]):
        reserves_by_fid[fid] = depo

    if mk and (mk not in reserves_by_mk or cu_rec(depo) > cu_rec(reserves_by_mk[mk])):
        reserves_by_mk[mk] = depo

print(f"reserves_by_fid: {len(reserves_by_fid)} faenas")
print(f"reserves_by_mk : {len(reserves_by_mk)} match_keys")
print("  match_keys:", sorted(reserves_by_mk.keys()))

# ── Inyectar en RAW ──────────────────────────────────────────────────────────
raw["reserves_by_fid"] = reserves_by_fid
raw["reserves_by_mk"]  = reserves_by_mk

new_raw_json = json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
new_html = html[:idx] + new_raw_json + html[end:]

# ── Agregar panel HTML en fc-inner (después del city tag) ────────────────────
RESERVES_HTML = """
    <!-- ⓪ RESERVAS Y RECURSOS (SERNAGEOMIN) -->
    <div id="fc-reservas-wrap" style="display:none;margin-bottom:12px">
      <div style="font-size:10px;font-weight:700;color:#34d399;letter-spacing:.05em;
                  margin-bottom:6px;padding-bottom:4px;border-bottom:1px solid var(--bg4)">
        ⛏️ RESERVAS Y RECURSOS Cu (SERNAGEOMIN)
      </div>
      <div id="fc-reservas-body"></div>
    </div>
"""

TARGET_CITY = '    <!-- Nearest city tag (populated by JS on mine change) -->'
if TARGET_CITY in new_html:
    new_html = new_html.replace(TARGET_CITY, RESERVES_HTML + "\n" + TARGET_CITY)
    print("Inserted reserves HTML panel ✓")
else:
    print("WARNING: city tag anchor not found, HTML not modified")

# ── Agregar función JS buildReservasPanel ─────────────────────────────────────
JS_FUNC = r"""
// ── RESERVAS Y RECURSOS Cu (SERNAGEOMIN) ─────────────────────────────────────
function buildReservasPanel(mk, d){
  const wrap = document.getElementById('fc-reservas-wrap');
  const body = document.getElementById('fc-reservas-body');
  if(!wrap || !body) return;

  const rv = (RAW.reserves_by_mk || {})[mk];
  if(!rv){
    wrap.style.display = 'none';
    return;
  }
  wrap.style.display = 'block';

  // Estimate mine life from reserves + production history
  let mineLifeResStr = '—', mineLifeRecStr = '—';
  if(d && d.history){
    const histVals = Object.values(d.history).filter(v=>v>0);
    const last5    = histVals.slice(-5);
    const avgProd  = last5.length ? last5.reduce((a,b)=>a+b,0)/last5.length : 0; // kt Cu/year
    if(avgProd > 0){
      if(rv.cu_res_t != null){
        const yrs = rv.cu_res_t / (avgProd * 1000);
        const col = yrs < 10 ? '#ef4444' : yrs < 20 ? '#f59e0b' : '#22c55e';
        mineLifeResStr = `<span style="color:${col};font-weight:700">${yrs.toFixed(1)} a</span>`;
      }
      if(rv.cu_rec_t != null){
        const yrs = rv.cu_rec_t / (avgProd * 1000);
        mineLifeRecStr = `<span style="color:#94a3b8">${yrs.toFixed(0)} a</span>`;
      }
    }
  }

  function fmtMt(v){ return v != null ? `${Number(v).toLocaleString('es-CL',{maximumFractionDigits:1})} Mt` : '—'; }
  function fmtLey(v){ return v != null ? `${Number(v*100).toFixed(3)}%` : '—'; }
  function fmtT(v){
    if(v == null) return '—';
    if(v >= 1e9) return `${(v/1e9).toFixed(2)} Gt`;
    if(v >= 1e6) return `${(v/1e6).toFixed(2)} Mt`;
    if(v >= 1e3) return `${(v/1e3).toFixed(1)} kt`;
    return `${v.toFixed(0)} t`;
  }

  const hasRes = rv.cu_res_mt != null || rv.cu_res_t != null;
  const hasRec = rv.cu_rec_mt != null || rv.cu_rec_t != null;

  body.innerHTML = `
    <div style="font-size:10px;color:var(--text2);margin-bottom:6px">
      <b style="color:var(--text)">${rv.deposito}</b>
      <span style="color:var(--text3);margin-left:5px">${rv.modelo || ''}</span>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:7px">
      <!-- RECURSOS -->
      <div style="background:rgba(52,211,153,0.07);border:1px solid rgba(52,211,153,0.2);
                  border-radius:6px;padding:7px 9px">
        <div style="font-size:9px;font-weight:700;color:#34d399;margin-bottom:4px;letter-spacing:.04em">RECURSOS TOTALES</div>
        <div style="font-size:11px;font-weight:700">${fmtMt(rv.cu_rec_mt)}</div>
        <div style="font-size:10px;color:var(--text3)">Ley: <b style="color:var(--text)">${fmtLey(rv.cu_rec_ley)}</b></div>
        <div style="font-size:10px;color:#34d399;font-weight:600">${fmtT(rv.cu_rec_t)} Cu</div>
        <div style="font-size:9px;color:var(--text3);margin-top:3px">Horizonte: ${mineLifeRecStr}</div>
      </div>
      <!-- RESERVAS -->
      <div style="background:rgba(245,158,11,0.07);border:1px solid rgba(245,158,11,0.2);
                  border-radius:6px;padding:7px 9px">
        <div style="font-size:9px;font-weight:700;color:#f59e0b;margin-bottom:4px;letter-spacing:.04em">RESERVAS PROBADAS+PROBABLES</div>
        <div style="font-size:11px;font-weight:700">${hasRes ? fmtMt(rv.cu_res_mt) : '<span style="color:var(--text3)">Sin datos</span>'}</div>
        <div style="font-size:10px;color:var(--text3)">Ley: <b style="color:var(--text)">${fmtLey(rv.cu_res_ley)}</b></div>
        <div style="font-size:10px;color:#f59e0b;font-weight:600">${fmtT(rv.cu_res_t)} Cu</div>
        <div style="font-size:9px;color:var(--text3);margin-top:3px">Vida útil est.: ${mineLifeResStr}</div>
      </div>
    </div>

    <div style="font-size:9px;color:var(--text3);display:flex;gap:8px;flex-wrap:wrap">
      <span>📍 ${rv.dist_km} km del marcador</span>
      <span style="color:${rv.estado.includes('producci')&&!rv.estado.toLowerCase().includes('para')
        ?'#22c55e':rv.estado.toLowerCase().includes('para')?'#ef4444':'#f59e0b'}">● ${rv.estado}</span>
      ${rv.referencia ? `<a href="${rv.referencia}" target="_blank"
        style="color:#38bdf8;text-decoration:none">Fuente ↗</a>` : ''}
    </div>
  `;
}
"""

# Inject before onFcMineChange
ANCHOR_JS = "function onFcMineChange(){"
if ANCHOR_JS in new_html:
    new_html = new_html.replace(ANCHOR_JS, JS_FUNC + "\n" + ANCHOR_JS)
    print("Inserted buildReservasPanel JS function ✓")
else:
    print("WARNING: JS anchor not found")

# ── Patch onFcMineChange to call buildReservasPanel ──────────────────────────
OLD_CALL = "  buildFcBestChart(d, mk);"
NEW_CALL = "  buildReservasPanel(mk, d);\n  buildFcBestChart(d, mk);"
if OLD_CALL in new_html:
    new_html = new_html.replace(OLD_CALL, NEW_CALL, 1)
    print("Patched onFcMineChange to call buildReservasPanel ✓")
else:
    print("WARNING: buildFcBestChart anchor not found")

# ── Also enrich installation popup with reserves data ────────────────────────
OLD_POPUP_ANCHOR = "      ${prodStr?`<div style=\"margin-top:5px;font-size:11px;color:#94a3b8\">"
POPUP_INJECT = (
    "      ${(()=>{"
    "const rv=(RAW.reserves_by_fid||{})[inst.id_faena];"
    "if(!rv) return '';"
    "const fmtT=v=>{ if(v==null) return '\\u2014'; if(v>=1e6) return (v/1e6).toFixed(2)+' Mt'; if(v>=1e3) return (v/1e3).toFixed(1)+' kt'; return v.toFixed(0)+' t'; };"
    "return '<div style=\"margin-top:5px;padding-top:5px;border-top:1px solid rgba(255,255,255,0.08);font-size:10px;color:#94a3b8\">'"
    "+'\\u26cf\\ufe0f <b style=\"color:#34d399\">Recursos Cu:</b> '+fmtT(rv.cu_rec_t)"
    "+(rv.cu_res_t!=null?' &nbsp;&middot;&nbsp; <b style=\"color:#f59e0b\">Reservas Cu:</b> '+fmtT(rv.cu_res_t):'')"
    "+'<div style=\"color:#64748b;font-size:9px;margin-top:2px\">'+rv.deposito+' &middot; SERNAGEOMIN</div></div>';"
    "})()}\n"
)

if OLD_POPUP_ANCHOR in new_html:
    new_html = new_html.replace(OLD_POPUP_ANCHOR,
                                POPUP_INJECT + OLD_POPUP_ANCHOR, 1)
    print("Patched installation popup with reserves data ✓")
else:
    print("WARNING: popup anchor not found — popup not patched")

# ── Escribir HTML ─────────────────────────────────────────────────────────────
HTML.write_text(new_html, encoding="utf-8")
print(f"\nHTML escrito: {HTML} ({len(new_html):,} chars)")

# ── Resumen de cobertura ──────────────────────────────────────────────────────
print("\n=== COBERTURA DE RESERVAS ===")
prod_mines_with_res = [mk for mk in reserves_by_mk if mk]
print(f"Minas con pronóstico Y reservas: {len(prod_mines_with_res)}")
for mk in sorted(prod_mines_with_res):
    rv = reserves_by_mk[mk]
    rec_str = f"{rv['cu_rec_t']/1e6:.2f} Mt Cu" if rv['cu_rec_t'] else "rec=—"
    res_str = f"{rv['cu_res_t']/1e6:.2f} Mt Cu" if rv['cu_res_t'] else "res=—"
    print(f"  {mk:<40} {rec_str:<15} {res_str}")
print(f"\nFaenas con datos de reservas (popup): {len(reserves_by_fid)}")
