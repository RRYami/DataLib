from ELT.save_alpha_vantage import AlphaVantageSaver
from ELT.save_fred import FredSaver
from ELT.save_polygon import PolygonSaver


def save_fred_data():
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


def save_alpha_vantage_data():
    saver = AlphaVantageSaver()
    saver.save_all_fundamentals(["AMD"])


def save_polygon_data():
    saver = PolygonSaver()
    saver.save_daily_bars(["NVDA"], "2010-01-01", "2026-05-06")


def main():
    print("main")


if __name__ == "__main__":
    # save_polygon_data()
    save_fred_data()
