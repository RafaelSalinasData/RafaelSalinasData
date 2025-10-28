
def save_text_panel(text: str, title: str, filename: str):
    """Guarda una imagen PNG con texto (sirve como 'screenshot' explicativo)."""
    plt.figure(figsize=(10, 6))
    plt.axis("off")
    plt.title(title)
    wrapped = "\n".join(textwrap.wrap(text, width=90))
    plt.text(0.01, 0.95, wrapped, va="top")
    out = OUTDIR / filename
    plt.savefig(out, bbox_inches="tight", dpi=150)
    plt.close()

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in COLUMNS_REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el CSV: {missing}")
    return df

def parse_datetime_safe(x: Any):
    try:
        return pd.to_datetime(x, errors="coerce", utc=True)
    except Exception:
        return pd.NaT

def normalize_row(row: pd.Series) -> Dict[str, Any]:
    # Limpieza mínima y tipificación para Mongo
    coord = row.get("tweet_coord")
    if pd.notna(coord):
        # coord puede venir como string tipo "[lat, lon]" o "lat,lon"
        try:
            if isinstance(coord, str):
                coord = coord.strip()
                coord = coord.replace("[", "").replace("]", "")
                parts = [float(p.strip()) for p in coord.split(",")]
                if len(parts) == 2:
                    coord = parts
                else:
                    coord = None
            elif isinstance(coord, (list, tuple)) and len(coord) == 2:
                coord = [float(coord[0]), float(coord[1])]
            else:
                coord = None
        except Exception:
            coord = None
    else:
        coord = None

    doc = {
        "airline_sentiment_confidence": float(row.get("airline_sentiment_confidence", 0) or 0),
        "negativereason": (row.get("negativereason") or None),
        "negativereason_confidence": float(row.get("negativereason_confidence", 0) or 0),
        "airline": (row.get("airline") or None),
        "airline_sentiment_gold": (row.get("airline_sentiment_gold") or None),
        "name": (row.get("name") or None),
        "negativereason_gold": (row.get("negativereason_gold") or None),
        "retweet_count": int(pd.to_numeric(row.get("retweet_count"), errors="coerce") or 0),
        "text": (row.get("text") or ""),
        "tweet_coord": coord,
        "tweet_created": parse_datetime_safe(row.get("tweet_created")),
        "tweet_location": (row.get("tweet_location") or None),
        "user_timezone": (row.get("user_timezone") or None),
    }
    return doc

# ===============
# 1) Conexión y DB
# ===============
client = MongoClient(args.mongo)
db = client[args.db]
col = db[args.collection]

# Creamos índices útiles
try:
    col.create_index([("airline", ASCENDING)])
    col.create_index([("tweet_created", ASCENDING)])
    col.create_index([("text", TEXT), ("negativereason", TEXT)])
except OperationFailure as e:
    print("Advertencia al crear índices:", e)

save_text_panel(
    text=f"Conexión exitosa a MongoDB: {args.mongo}\nBase de datos: {args.db}\nColección: {args.collection}",
    title="Caso 1: Conexión a la plataforma NoSQL (MongoDB)",
    filename="01_conexion_mongo.png"
)

# =======================
# 2) Cargar y explorar CSV
# =======================
df = pd.read_csv(args.csv, encoding="utf-8", low_memory=False)
df = ensure_required_columns(df)

# Convertimos fecha
if "tweet_created" in df.columns:
    df["tweet_created"] = df["tweet_created"].apply(parse_datetime_safe)

# Asegurar al menos 1000 registros
n_rows = len(df)
sample_size = min(max(1000, 1000), n_rows)  # al menos 1000 si hay suficientes
sample_df = df.head(sample_size).copy()

# Exploración: frecuencia de palabras clave en texto
def contains_kw(text, kw):
    try:
        return kw.lower() in str(text).lower()
    except Exception:
        return False

kw_counts = {kw: sample_df["text"].apply(lambda t: contains_kw(t, kw)).sum() for kw in KEYWORDS}
kw_items = sorted(kw_counts.items(), key=lambda x: x[1], reverse=True)

# Gráfica 1: Palabras clave
plt.figure(figsize=(8,6))
plt.bar([k for k,_ in kw_items], [v for _,v in kw_items])
plt.title("Frecuencia de 5 palabras clave en el CSV")
plt.xlabel("Palabra clave")
plt.ylabel("Frecuencia")
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig(OUTDIR / "02_exploracion_keywords.png", dpi=150)
plt.close()

# Gráfica 2: Top aerolíneas
top_airlines = sample_df["airline"].value_counts().head(10)
plt.figure(figsize=(8,6))
top_airlines.plot(kind="bar")
plt.title("Top aerolíneas por volumen de tweets (muestra)")
plt.xlabel("Aerolínea")
plt.ylabel("Conteo")
plt.tight_layout()
plt.savefig(OUTDIR / "03_exploracion_airlines.png", dpi=150)
plt.close()

save_text_panel(
    text="Exploración del CSV: se graficó la frecuencia de 5 keywords (delay, cancel, service, price, baggage) y el top de aerolíneas por volumen en la muestra.",
    title="Caso 2: Exploración del archivo de tweets",
    filename="04_exploracion_panel.png"
)

# ========================
# 3) Importar a MongoDB
# ========================
records = [normalize_row(r) for _, r in sample_df.iterrows()]
if records:
    # Limpiar colección previa opcionalmente (descomentar si quieres forzar limpio)
    # col.delete_many({})
    col.insert_many(records, ordered=False)

# Confirmación de insert
count_in_db = col.count_documents({})
save_text_panel(
    text=f"Importación completa.\nRegistros añadidos en esta ejecución: {len(records)}\nDocumentos totales en la colección: {count_in_db}",
    title="Caso 3: Importación de ≥1000 tweets a MongoDB",
    filename="05_importacion_panel.png"
)

# Exportar una copia del sample limpio (opcional)
pd.DataFrame(records).to_csv(OUTDIR / "tweets_importados_sample.csv", index=False)

# ========================
# 4) Filtros de búsqueda
# ========================

# Filtro A: Tweets con alta confianza de sentimiento y con razón negativa conocida
filtro_a_query = {
    "airline_sentiment_confidence": {"$gte": args.min_conf},
    "negativereason": {"$ne": None}
}
filtro_a = list(col.find(filtro_a_query, {"_id": 0, "negativereason": 1}))
# Conteo por negativereason
from collections import Counter
cnt_a = Counter([d.get("negativereason") for d in filtro_a if d.get("negativereason")])

labels_a, values_a = zip(*sorted(cnt_a.items(), key=lambda x: x[1], reverse=True)) if cnt_a else ([],[])

plt.figure(figsize=(10,6))
plt.bar(labels_a, values_a)
plt.title(f"Filtro A: Negativereason (confianza ≥ {args.min_conf})")
plt.xlabel("Razón negativa")
plt.ylabel("Conteo")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(OUTDIR / "06_filtroA_negativereason.png", dpi=150)
plt.close()

save_text_panel(
    text=f"Filtro A ejecutado: airline_sentiment_confidence ≥ {args.min_conf} y negativereason ≠ None.\nSe grafica la distribución de las razones negativas.",
    title="Caso 4.1: Filtro A (confianza alta + razón negativa)",
    filename="06_filtroA_panel.png"
)

# Filtro B: Tweets de una aerolínea específica que mencionan alguna de las 5 keywords
regex_keywords = "|".join([f"(?i){kw}" for kw in KEYWORDS])
filtro_b_query = {
    "airline": args.airline_filter,
    "text": {"$regex": regex_keywords}
}
filtro_b = list(col.find(filtro_b_query, {"_id": 0, "text": 1}))

# Conteo por palabra clave detectada
def count_kw_in_texts(docs, keywords):
    counts = {kw: 0 for kw in keywords}
    for d in docs:
        t = d.get("text", "")
        tl = t.lower()
        for kw in keywords:
            if kw.lower() in tl:
                counts[kw] += 1
    return counts

cnt_b = count_kw_in_texts(filtro_b, KEYWORDS)
labels_b, values_b = zip(*sorted(cnt_b.items(), key=lambda x: x[1], reverse=True)) if cnt_b else ([], [])

plt.figure(figsize=(8,6))
plt.bar(labels_b, values_b)
plt.title(f"Filtro B: '{args.airline_filter}' + keywords")
plt.xlabel("Keyword")
plt.ylabel("Conteo")
plt.tight_layout()
plt.savefig(OUTDIR / "07_filtroB_keywords.png", dpi=150)
plt.close()

save_text_panel(
    text=f"Filtro B ejecutado: airline == '{args.airline_filter}' y texto contiene alguna de: {', '.join(KEYWORDS)}.",
    title="Caso 4.2: Filtro B (aerolínea específica + keywords)",
    filename="07_filtroB_panel.png"
)

# ========================
# 5) Resumen final
# ========================
save_text_panel(
    text=(
        "Resumen del flujo:\n"
        "1) Conexión a MongoDB y creación de BD/colección + índices.\n"
        "2) Exploración del CSV con 5 keywords y top aerolíneas.\n"
        "3) Importación de ≥1000 documentos a MongoDB.\n"
        "4) Filtros de búsqueda:\n"
        "   - A) Alta confianza + razón negativa -> distribución de negativereason.\n"
        "   - B) Aerolínea específica + keywords -> distribución de keywords.\n"
        f"Archivos generados en: {OUTDIR.resolve()}"
    ),
    title="Caso 5: Resumen y outputs",
    filename="08_resumen_panel.png"
)

print("Listo. Revisa la carpeta de salida:", OUTDIR.resolve())
