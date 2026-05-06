from ELT.save_fred import FredSaver


def main():
    saver = FredSaver()

    # --- Save / incrementally update all four components ---
    print("Saving FRED yield curve data to Parquet...")
    saver.save_all(lookback_days=7)
    print("Done.\n")

    # --- Read back and inspect ---
    print("=== Treasury Constant Maturity (latest 5 rows, wide) ===")
    df = saver.read_treasury_constant_maturity(wide=True)
    if df is not None:
        print(df.tail(5))
    print()

    print("=== GSW Forward Rates (latest 5 rows, wide) ===")
    df = saver.read_gsw_forward_rates(wide=True)
    if df is not None:
        print(df.tail(5))
    print()

    print("=== GSW Term Premiums (latest 5 rows, long) ===")
    df = saver.read_gsw_term_premiums(wide=False)
    if df is not None:
        print(df.tail(5))
    print()

    # --- Re-run to verify idempotence ---
    print("Re-running save_all() to verify idempotence...")
    saver.save_all(lookback_days=7)
    print("Idempotence check complete — no duplicates should have been added.")


if __name__ == "__main__":
    main()
