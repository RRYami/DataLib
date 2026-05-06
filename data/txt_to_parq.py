import polars as pl

# Define input and output file paths
txt_file_path = "data/SIC_NAIC.csv"
parquet_file_path = "data/sic_naics.parquet"


def convert_to_parquet(
    from_dir: str, to_dir: str, file_type: str, seperator: str
):
    match file_type:
        case "txt" | "csv":
            try:
                df = pl.read_csv(from_dir, separator=seperator)
                df.write_parquet(to_dir, compression="zstd")
            except FileNotFoundError as e:
                print(f"Error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
            else:
                print(
                    f"Successfully converted {from_dir} to {to_dir} using Polars."
                )
        case _:
            print("File type not supported")


if __name__ == "__main__":
    convert_to_parquet(
        from_dir=txt_file_path,
        to_dir=parquet_file_path,
        file_type="csv",
        seperator=",",
    )
