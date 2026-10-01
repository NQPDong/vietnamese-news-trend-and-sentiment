def drop_exact_duplicates(df, primary_key_cols=None, secondary_key_cols=None):
    """
    Khử trùng lặp chính xác (Exact Deduplication) 2 bước trên Spark DataFrame:
    - Bước 1: Khử trùng theo khóa chính (mặc định: url).
    - Bước 2: Khử trùng theo khóa phụ nội dung (mặc định: title, source).
    """
    if primary_key_cols is None:
        primary_key_cols = ["url"]
    if secondary_key_cols is None:
        secondary_key_cols = ["title", "source"]

    # Khử trùng theo khóa chính
    dedup_df = df.dropDuplicates(primary_key_cols)

    # Khử trùng theo khóa phụ
    dedup_df = dedup_df.dropDuplicates(secondary_key_cols)

    return dedup_df
