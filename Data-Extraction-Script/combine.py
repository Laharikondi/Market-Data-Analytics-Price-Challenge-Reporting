import pandas as pd

# ====== Input Files ======
file1 = "market_data.csv"       # Replace with your first CSV filename
file2 = "D:/Lahari/lahari/TCS PSP/Market price challenge/Data sets/Alpha-vintage.csv"       # Replace with your second CSV filename

# ====== Output File ======
output_file = "combined_market_data.csv"

# ====== Read both files ======
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# ====== Align Columns (in case they differ slightly) ======
for col in df1.columns:
    if col not in df2.columns:
        df2[col] = None
for col in df2.columns:
    if col not in df1.columns:
        df1[col] = None

# ====== Combine both ======
combined_df = pd.concat([df1, df2], ignore_index=True)

# ====== Keep same column order ======
combined_df = combined_df[df1.columns]

# ====== Save final output ======
combined_df.to_csv(output_file, index=False)

print(f"[SUCCESS] Combined both files into {output_file}")
print(f"→ Total rows in final file: {len(combined_df)}")
