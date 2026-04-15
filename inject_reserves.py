"""
inject_reserves.py  v3
Cruza las faenas del mapa (con match_key) con el GDB de SERNAGEOMIN e inyecta
datos de recursos/reservas en index.html.

LÓGICA:
  1. Para cada match_key con OVERRIDE por nombre → busca el depósito por nombre exacto en GDB.
  2. Para el resto → matching por proximidad (faena→GDB, umbral 30 km),
     eligiendo el depósito con mayor Cu_rec_t si hay varios dentro del umbral.
  3. Idempotente: reemplaza los bloques JS/RAW ya existentes si el script se vuelve a correr.
"""

import json, re, pyogrio
import numpy as np
from pathlib import Path

# ── Rutas ─────────────────────────────────────────────────────────────────────
HTML        = Path(__file__).parent / "index.html"
GDB         = Path(__file__).parent.parent / "Estudio" / "EMC_SNGM.gdb"
DIST_KM_THR = 30

# ── Overrides completos: match_key → nombre EXACTO en BD_EMC_MET_IND ─────────
# False = no existe en GDB, omitir.
# String = usar ese nombre exacto (todos los match_keys mapeados explícitamente).
NAME_OVERRIDES = {
    "altos de punitaqui":          "Mantos de Punitaqui",
    "andacollo":                   "Carmen de Andacollo",
    "andina":                      "Andina",
    "antucoya":                    "Antucoya",
    "atacama kozan":               "Candelaria",        # opción dentro del distrito Candelaria
    "candelaria":                  "Candelaria",
    "caserones":                   "Caserones",
    "centinela_centinela_sulfuros_": "Centinela",
    "centinela_centinela_óxidos_":  "Centinela",
    "cerro colorado":              "Cerro Colorado",
    "cerro negro":                 "Cerro Negro",
    "chuquicamata":                "Chuquicamata",
    "collahuasi":                  "Collahuasi",
    "el abra":                     "El Abra",
    "el soldado":                  "El Soldado",
    "el teniente":                 "El Teniente",
    "escondida":                   "Escondida",
    "franke":                      "Franke",
    "gabriela mistral":            "Gabriela Mistral",
    "lomas bayas":                 "Lomas Bayas",
    "los bronces":                 "Los Bronces Subterráneo",   # tiene más recursos publicados
    "los_pelambres":               "Los Pelambres",
    "mantos blancos":              "Mantos Blancos",
    "mantoverde":                  "Mantoverde",
    "michilla":                    "Distrito Michilla",
    "ministro hales":              "Ministro Hales",
    "pampa camarones":             "Pampa Camarones",
    "quebrada blanca":             "Quebrada Blanca",
    "radomiro tomic":              "Radomiro Tomic",
    "salvador":                    "Salvador",
    "sierra gorda":                "Sierra Gorda",
    "spence":                      "Spence",
    "tres valles":                 False,               # no existe en GDB
    "zaldivar":                    "Zaldivar",
}

# ── Leer GDB ──────────────────────────────────────────────────────────────────
gdf = pyogrio.read_dataframe(str(GDB), layer="BD_EMC_MET_IND").dropna(subset=["LATITUD","LONGITUD"])
dep_coords = gdf[["LATITUD","LONGITUD"]].values   # shape (N,2)

# ── Utilidades ────────────────────────────────────────────────────────────────
def cu_fields(row):
    for n in [1,2,3,4]:
        if str(row.get(f"CRITICO_{n}","")).strip() == "Cobre":
            return {
                "cu_rec_mt":  row.get(f"RECURSOS_TOTALES_{n}_TONELAJE_MT"),
                "cu_rec_ley": row.get(f"RECURSOS_TOTALES_{n}_LEY_PCT"),
                "cu_rec_t":   row.get(f"CONTENIDO_METALICO_RECURSO_{n}_T"),
                "cu_res_mt":  row.get(f"RESERVAS_TOTALES_{n}_TONELAJE_MT"),
                "cu_res_ley": row.get(f"RESERVAS_TOTALES_{n}_LEY_PCT"),
                "cu_res_t":   row.get(f"CONTENIDO_METALICO_RESERVA_{n}_T"),
            }
    return {}

def primary_fields(row):
    for n in [1,2,3,4]:
        crit = str(row.get(f"CRITICO_{n}","")).strip()
        if crit and crit.lower() != "nan":
            return {
                "mineral_prim": crit,
                "prim_rec_t":   row.get(f"CONTENIDO_METALICO_RECURSO_{n}_T"),
                "prim_res_t":   row.get(f"CONTENIDO_METALICO_RESERVA_{n}_T"),
                "prim_rec_mt":  row.get(f"RECURSOS_TOTALES_{n}_TONELAJE_MT"),
                "prim_rec_ley": row.get(f"RECURSOS_TOTALES_{n}_LEY_PCT"),
            }
    return {}

def clean(v):
    if v is None: return None
    try:
        f = float(v)
        return None if (np.isnan(f) or f == 0.0) else round(f, 4)
    except Exception:
        return None

def minerales_str(row):
    return " · ".join(filter(
        lambda x: x and x.lower() != "nan",
        [str(row.get(f"CRITICO_{n}","")) for n in [1,2,3,4]]
    ))

def build_depo(row, dist):
    cu = cu_fields(row)
    pf = primary_fields(row)
    return {
        "deposito":     str(row["NOMBRE_DEPOSITO"]),
        "estado":       str(row.get("ESTADO_DEPOSITO","")),
        "modelo":       str(row.get("MODELO_DEPOSITO","")),
        "minerales":    minerales_str(row),
        "cu_rec_mt":    clean(cu.get("cu_rec_mt")),
        "cu_rec_ley":   clean(cu.get("cu_rec_ley")),
        "cu_rec_t":     clean(cu.get("cu_rec_t")),
        "cu_res_mt":    clean(cu.get("cu_res_mt")),
        "cu_res_ley":   clean(cu.get("cu_res_ley")),
        "cu_res_t":     clean(cu.get("cu_res_t")),
        "mineral_prim": pf.get("mineral_prim"),
        "prim_rec_t":   clean(pf.get("prim_rec_t")),
        "prim_res_t":   clean(pf.get("prim_res_t")),
        "prim_rec_mt":  clean(pf.get("prim_rec_mt")),
        "prim_rec_ley": clean(pf.get("prim_rec_ley")),
        "referencia":   str(row.get("REFERENCIA","")) if row.get("REFERENCIA") else None,
        "dist_km":      round(dist, 1),
        "lat":          float(row["LATITUD"]),
        "lon":          float(row["LONGITUD"]),
    }

# ── Leer HTML y extraer RAW ───────────────────────────────────────────────────
html = HTML.read_text(encoding="utf-8")
tag  = "const RAW = "
idx  = html.index(tag) + len(tag)
brace = 0; end = idx
for i, c in enumerate(html[idx:], idx):
    if c == "{": brace += 1
    elif c == "}": brace -= 1
    if brace == 0: end = i+1; break
raw    = json.loads(html[idx:end])
faenas = raw["faenas"]

# ── Matching: faena → GDB ────────────────────────────────────────────────────
reserves_by_mk  = {}
reserves_by_fid = {}

faenas_with_mk = [f for f in faenas if f.get("match_key")]
print(f"Faenas con match_key: {len(faenas_with_mk)}")

for f in faenas_with_mk:
    mk   = f["match_key"]
    fid  = f["id_faena"]
    flat, flon = f["lat"], f["lon"]

    # ── Override por nombre ──────────────────────────────────────────────────
    if mk in NAME_OVERRIDES:
        override = NAME_OVERRIDES[mk]
        if override is False:
            print(f"  SKIP (no GDB):   {mk}")
            continue

        r = gdf[gdf["NOMBRE_DEPOSITO"].str.strip() == override]
        if len(r) == 0:
            print(f"  OVERRIDE NOT FOUND: '{override}' for '{mk}'")
            continue

        row  = r.iloc[0]
        d    = float(np.sqrt((float(row["LATITUD"])-flat)**2 + (float(row["LONGITUD"])-flon)**2) * 111.0)
        depo = build_depo(row, d)
        reserves_by_mk[mk]   = depo
        reserves_by_fid[fid] = depo
        cu_str = f"{depo['cu_rec_t']/1e6:.1f}Mt" if depo.get("cu_rec_t") else "—"
        print(f"  OVERRIDE:  {mk:<43} → '{override}' {d:.1f}km  Cu={cu_str}")
        continue

    # ── Matching por distancia: elegir el de mayor Cu_rec_t dentro del umbral ─
    dists = np.sqrt((dep_coords[:,0]-flat)**2 + (dep_coords[:,1]-flon)**2) * 111.0
    in_thr = np.where(dists <= DIST_KM_THR)[0]

    if len(in_thr) == 0:
        print(f"  NO MATCH (>{DIST_KM_THR}km): {mk}  closest='{gdf.iloc[int(np.argmin(dists))]['NOMBRE_DEPOSITO']}' {dists.min():.1f}km")
        continue

    # Pick the one with highest Cu content (fallback to closest if all Cu=0)
    cu_vals = []
    for i in in_thr:
        row_i = gdf.iloc[i]
        cu = cu_fields(row_i)
        v   = float(cu.get("cu_rec_t") or 0)
        if np.isnan(v): v = 0.0
        cu_vals.append(v)

    best_i = in_thr[int(np.argmax(cu_vals))] if max(cu_vals) > 0 else in_thr[int(np.argmin(dists[in_thr]))]
    row  = gdf.iloc[best_i]
    depo = build_depo(row, float(dists[best_i]))
    reserves_by_mk[mk]   = depo
    reserves_by_fid[fid] = depo
    cu_str = f"{depo['cu_rec_t']/1e6:.1f}Mt" if depo.get("cu_rec_t") else "—"
    print(f"  MATCH:     {mk:<43} → '{row['NOMBRE_DEPOSITO']}' {dists[best_i]:.1f}km  Cu={cu_str}")

# Enriquecer resto de faenas (popup de instalaciones)
for f in faenas:
    fid = f["id_faena"]
    if fid in reserves_by_fid:
        continue
    flat, flon = f["lat"], f["lon"]
    dists  = np.sqrt((dep_coords[:,0]-flat)**2 + (dep_coords[:,1]-flon)**2) * 111.0
    in_thr = np.where(dists <= DIST_KM_THR)[0]
    if len(in_thr) == 0:
        continue
    cu_vals = []
    for i in in_thr:
        cu = cu_fields(gdf.iloc[i])
        v  = float(cu.get("cu_rec_t") or 0)
        cu_vals.append(0.0 if np.isnan(v) else v)
    best_i = in_thr[int(np.argmax(cu_vals))] if max(cu_vals) > 0 else in_thr[int(np.argmin(dists[in_thr]))]
    depo = build_depo(gdf.iloc[best_i], float(dists[best_i]))
    if depo.get("cu_rec_t") or depo.get("prim_rec_t"):
        reserves_by_fid[fid] = depo

print(f"\nreserves_by_mk : {len(reserves_by_mk)}")
print(f"reserves_by_fid: {len(reserves_by_fid)}")

# ── Actualizar RAW ────────────────────────────────────────────────────────────
raw["reserves_by_mk"]  = reserves_by_mk
raw["reserves_by_fid"] = reserves_by_fid
new_raw = json.dumps(raw, ensure_ascii=False, separators=(",",":"))
new_html = html[:idx] + new_raw + html[end:]

# ── JS: buildReservasPanel ────────────────────────────────────────────────────
JS_FUNC = r"""
// ── RESERVAS Y RECURSOS Cu (SERNAGEOMIN) ─────────────────────────────────────
function buildReservasPanel(mk, d){
  const wrap = document.getElementById('fc-reservas-wrap');
  const body = document.getElementById('fc-reservas-body');
  if(!wrap || !body) return;
  const rv = (RAW.reserves_by_mk||{})[mk];
  if(!rv){ wrap.style.display='none'; return; }
  wrap.style.display='block';

  let lifeResStr='—', lifeRecStr='—';
  if(d && d.history){
    const hv=Object.values(d.history).filter(v=>v>0);
    const last5=hv.slice(-5);
    const avg=last5.length?last5.reduce((a,b)=>a+b,0)/last5.length:0;
    if(avg>0){
      const col=y=>y<10?'#ef4444':y<20?'#f59e0b':'#22c55e';
      if(rv.cu_res_t!=null){const y=rv.cu_res_t/(avg*1000);lifeResStr=`<span style="color:${col(y)};font-weight:700">${y.toFixed(1)} a</span>`;}
      if(rv.cu_rec_t!=null){const y=rv.cu_rec_t/(avg*1000);lifeRecStr=`<span style="color:#94a3b8">${y.toFixed(0)} a</span>`;}
    }
  }

  const fmtMt =v=>v!=null?`${Number(v).toLocaleString('es-CL',{maximumFractionDigits:1})} Mt`:'—';
  const fmtLey=v=>v!=null?`${(Number(v)*100).toFixed(3)}%`:'—';
  const fmtT  =v=>{if(v==null)return'—';if(v>=1e9)return`${(v/1e9).toFixed(2)} Gt`;if(v>=1e6)return`${(v/1e6).toFixed(2)} Mt`;if(v>=1e3)return`${(v/1e3).toFixed(1)} kt`;return`${v.toFixed(0)} t`;};

  const hasCuRec=rv.cu_rec_mt!=null||rv.cu_rec_t!=null;
  const hasCuRes=rv.cu_res_mt!=null||rv.cu_res_t!=null;
  const primLabel=rv.mineral_prim&&!hasCuRec?` (${rv.mineral_prim})`:'';
  const recMt =hasCuRec?rv.cu_rec_mt:rv.prim_rec_mt;
  const recLey=hasCuRec?rv.cu_rec_ley:rv.prim_rec_ley;
  const recT  =hasCuRec?rv.cu_rec_t:rv.prim_rec_t;
  const resMt =hasCuRes?rv.cu_res_mt:null;
  const resT  =hasCuRes?rv.cu_res_t:rv.prim_res_t;
  const resLey=hasCuRes?rv.cu_res_ley:null;
  const estadoColor=rv.estado&&rv.estado.toLowerCase().includes('producci')&&!rv.estado.toLowerCase().includes('para')?'#22c55e':rv.estado&&rv.estado.toLowerCase().includes('para')?'#ef4444':'#f59e0b';

  body.innerHTML=`
    <div style="font-size:10px;color:var(--text2);margin-bottom:6px">
      <b style="color:var(--text)">${rv.deposito}</b>
      <span style="color:var(--text3);margin-left:5px">${rv.modelo||''}</span>
      ${rv.minerales?`<div style="color:var(--text3);font-size:9px;margin-top:2px">⛏ ${rv.minerales}</div>`:''}
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:7px">
      <div style="background:rgba(52,211,153,0.07);border:1px solid rgba(52,211,153,0.2);border-radius:6px;padding:7px 9px">
        <div style="font-size:9px;font-weight:700;color:#34d399;margin-bottom:4px;letter-spacing:.04em">RECURSOS TOTALES${primLabel}</div>
        <div style="font-size:11px;font-weight:700">${fmtMt(recMt)}</div>
        <div style="font-size:10px;color:var(--text3)">Ley: <b style="color:var(--text)">${fmtLey(recLey)}</b></div>
        <div style="font-size:10px;color:#34d399;font-weight:600">${fmtT(recT)} Cu</div>
        <div style="font-size:9px;color:var(--text3);margin-top:3px">Horizonte: ${lifeRecStr}</div>
      </div>
      <div style="background:rgba(245,158,11,0.07);border:1px solid rgba(245,158,11,0.2);border-radius:6px;padding:7px 9px">
        <div style="font-size:9px;font-weight:700;color:#f59e0b;margin-bottom:4px;letter-spacing:.04em">RESERVAS PROBADAS+PROBABLES</div>
        <div style="font-size:11px;font-weight:700">${resMt!=null?fmtMt(resMt):'<span style="color:var(--text3);font-size:9px">Sin datos publicados</span>'}</div>
        <div style="font-size:10px;color:var(--text3)">Ley: <b style="color:var(--text)">${fmtLey(resLey)}</b></div>
        <div style="font-size:10px;color:#f59e0b;font-weight:600">${fmtT(resT)} Cu</div>
        <div style="font-size:9px;color:var(--text3);margin-top:3px">Vida útil est.: ${lifeResStr}</div>
      </div>
    </div>
    <div style="font-size:9px;color:var(--text3);display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <span>📍 ${rv.dist_km} km del marcador</span>
      <span style="color:${estadoColor}">● ${rv.estado||'—'}</span>
      ${rv.referencia?`<a href="${rv.referencia}" target="_blank" style="color:#38bdf8;text-decoration:none">Fuente SERNAGEOMIN ↗</a>`:''}
    </div>
  `;
}
"""

# Reemplazar bloque JS existente (si ya estaba) o insertar
OLD_JS_MARKER = "// ── RESERVAS Y RECURSOS Cu (SERNAGEOMIN)"
ANCHOR_JS     = "function onFcMineChange(){"

if OLD_JS_MARKER in new_html:
    # Encontrar inicio del bloque (comentario) hasta el anchor
    start_js = new_html.index(OLD_JS_MARKER)
    end_js   = new_html.index("\n" + ANCHOR_JS, start_js)
    new_html = new_html[:start_js] + JS_FUNC.lstrip("\n") + new_html[end_js:]
    print("Updated buildReservasPanel JS ✓")
elif ANCHOR_JS in new_html:
    new_html = new_html.replace(ANCHOR_JS, JS_FUNC + "\n" + ANCHOR_JS)
    print("Inserted buildReservasPanel JS ✓")
else:
    print("WARNING: JS anchor not found")

# ── Patch onFcMineChange ──────────────────────────────────────────────────────
OLD_CALL = "  buildFcBestChart(d, mk);"
NEW_CALL = "  buildReservasPanel(mk, d);\n  buildFcBestChart(d, mk);"
if NEW_CALL not in new_html:
    new_html = new_html.replace(OLD_CALL, NEW_CALL, 1)
    print("Patched onFcMineChange ✓")
else:
    print("onFcMineChange already patched — skipped")

# ── Panel HTML (idempotente) ──────────────────────────────────────────────────
RESERVES_PANEL = """
    <!-- ⓪ RESERVAS Y RECURSOS (SERNAGEOMIN) -->
    <div id="fc-reservas-wrap" style="display:none;margin-bottom:12px">
      <div style="font-size:10px;font-weight:700;color:#34d399;letter-spacing:.05em;
                  margin-bottom:6px;padding-bottom:4px;border-bottom:1px solid var(--bg4)">
        ⛏️ RECURSOS Y RESERVAS (SERNAGEOMIN)
      </div>
      <div id="fc-reservas-body"></div>
    </div>
"""
ANCHOR_CITY = '    <!-- Nearest city tag (populated by JS on mine change) -->'
if "fc-reservas-wrap" not in new_html:
    if ANCHOR_CITY in new_html:
        new_html = new_html.replace(ANCHOR_CITY, RESERVES_PANEL + "\n" + ANCHOR_CITY)
        print("Inserted reserves HTML panel ✓")
    else:
        print("WARNING: city tag anchor not found")
else:
    print("HTML panel already present — skipped")

# ── Popup instalaciones (idempotente) ─────────────────────────────────────────
OLD_POPUP    = '      ${prodStr?`<div style=\"margin-top:5px;font-size:11px;color:#94a3b8\">'
POPUP_INJECT = (
    "      ${(()=>{"
    "const rv=(RAW.reserves_by_fid||{})[inst.id_faena];"
    "if(!rv) return '';"
    "const fmtT=v=>{if(v==null)return'\\u2014';if(v>=1e6)return(v/1e6).toFixed(2)+' Mt';if(v>=1e3)return(v/1e3).toFixed(1)+' kt';return v.toFixed(0)+' t';};"
    "const recT=rv.cu_rec_t!=null?rv.cu_rec_t:rv.prim_rec_t;"
    "const resT=rv.cu_res_t!=null?rv.cu_res_t:rv.prim_res_t;"
    "const lbl=rv.cu_rec_t!=null?'Cu':(rv.mineral_prim||'');"
    "return '<div style=\"margin-top:5px;padding-top:5px;border-top:1px solid rgba(255,255,255,0.08);font-size:10px;color:#94a3b8\">'"
    "+'\\u26cf\\ufe0f <b style=\"color:#34d399\">Recursos '+lbl+':</b> '+fmtT(recT)"
    "+(resT!=null?' &nbsp;&middot;&nbsp; <b style=\"color:#f59e0b\">Reservas:</b> '+fmtT(resT):'')"
    "+'<div style=\"color:#64748b;font-size:9px;margin-top:2px\">'+rv.deposito+' &middot; SERNAGEOMIN</div></div>';"
    "})()}\n"
)
popup_already = "reserves_by_fid" in new_html[new_html.find("${(()=>{"):new_html.find("${(()=>{")+500] if "${(()=>{" in new_html else False
if not popup_already:
    if OLD_POPUP in new_html:
        new_html = new_html.replace(OLD_POPUP, POPUP_INJECT + OLD_POPUP, 1)
        print("Patched installation popup ✓")
    else:
        print("WARNING: popup anchor not found")
else:
    print("Installation popup already patched — skipped")

# ── Escribir ──────────────────────────────────────────────────────────────────
HTML.write_text(new_html, encoding="utf-8")
print(f"\nHTML escrito: {len(new_html):,} chars")

# ── Resumen final ─────────────────────────────────────────────────────────────
print(f"\n{'MATCH_KEY':<45} {'DEPOSITO GDB':<35} {'Cu_rec':>10} {'Cu_res':>10}")
print("─"*105)
all_mks = sorted({f['match_key'] for f in faenas if f.get('match_key')})
for mk in all_mks:
    rv = reserves_by_mk.get(mk)
    if rv:
        rec = f"{rv['cu_rec_t']/1e6:.1f}Mt" if rv.get('cu_rec_t') else "—"
        res = f"{rv['cu_res_t']/1e6:.1f}Mt" if rv.get('cu_res_t') else "—"
        print(f"  {mk:<43} {rv['deposito']:<35} {rec:>10} {res:>10}")
    else:
        print(f"  {mk:<43} {'— SIN DATOS —':<35}")
