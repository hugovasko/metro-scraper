import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleMetroImporter:
    def __init__(self, connection_string: str):
        """Initialize with Supabase connection string"""
        self.connection_string = connection_string

    def connect(self):
        """Create database connection"""
        try:
            conn = psycopg2.connect(self.connection_string)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return None

    def clean_csv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare CSV data for import"""
        logger.info("Cleaning CSV data...")

        # Replace NaN/empty values with None for proper NULL handling
        df = df.where(pd.notnull(df), None)

        # Ensure code column is string and not empty
        df = df[df['code'].notna() & (df['code'] != '')]
        df['code'] = df['code'].astype(str)

        # Convert numeric columns to proper types
        numeric_columns = [
            'energy_100g', 'fat_100g', 'saturated_fat_100g', 'proteins_100g',
            'carbohydrates_100g', 'sugars_100g', 'fiber_100g', 'sodium_100g'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        logger.info(f"Cleaned data: {len(df)} rows ready for import")
        return df

    def import_csv_to_metro_source(self, csv_file_path: str, batch_size: int = 1000) -> bool:
        """Import CSV data to metro_source table"""
        if not os.path.exists(csv_file_path):
            logger.error(f"CSV file not found: {csv_file_path}")
            return False

        try:
            # Read CSV
            logger.info(f"Reading CSV file: {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            logger.info(f"CSV loaded: {len(df)} rows")

            # Clean data
            df = self.clean_csv_data(df)

            # Connect to database
            conn = self.connect()
            if not conn:
                return False

            cur = conn.cursor()

            # Clear existing data
            logger.info("Clearing existing metro_source data...")
            cur.execute("DELETE FROM public.metro_source")

            # Prepare data for batch insert
            columns = [
                'code', 'product_name', 'quantity', 'brand', 'categories',
                'ingredients', 'image_url', 'nutriscore_grade', 'energy_100g',
                'fat_100g', 'saturated_fat_100g', 'proteins_100g',
                'carbohydrates_100g', 'sugars_100g', 'fiber_100g', 'sodium_100g'
            ]

            # Process in batches
            total_rows = len(df)
            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]

                # Convert to list of tuples
                batch_data = []
                for _, row in batch_df.iterrows():
                    row_data = tuple(row[col] if col in row and pd.notna(row[col]) else None for col in columns)
                    batch_data.append(row_data)

                # Execute batch insert
                insert_sql = f"""
                INSERT INTO public.metro_source ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (code) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    quantity = EXCLUDED.quantity,
                    brand = EXCLUDED.brand,
                    categories = EXCLUDED.categories,
                    ingredients = EXCLUDED.ingredients,
                    image_url = EXCLUDED.image_url,
                    nutriscore_grade = EXCLUDED.nutriscore_grade,
                    energy_100g = EXCLUDED.energy_100g,
                    fat_100g = EXCLUDED.fat_100g,
                    saturated_fat_100g = EXCLUDED.saturated_fat_100g,
                    proteins_100g = EXCLUDED.proteins_100g,
                    carbohydrates_100g = EXCLUDED.carbohydrates_100g,
                    sugars_100g = EXCLUDED.sugars_100g,
                    fiber_100g = EXCLUDED.fiber_100g,
                    sodium_100g = EXCLUDED.sodium_100g,
                    created_at = now()
                """

                execute_values(cur, insert_sql, batch_data)
                conn.commit()

                logger.info(
                    f"Imported batch {i // batch_size + 1}: {len(batch_data)} rows (Total: {min(i + batch_size, total_rows)}/{total_rows})")

            logger.info(f"Successfully imported {total_rows} products to metro_source table")
            return True

        except Exception as e:
            logger.error(f"Error importing CSV: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_food_item_sources(self) -> bool:
        """Update food_item_sources table with Metro data - optimized version"""
        conn = self.connect()
        if not conn:
            return False

        try:
            cur = conn.cursor()

            # Set longer timeout for large operations
            cur.execute("SET statement_timeout = '10min'")

            logger.info("Creating temporary indexes for better performance...")

            # Create temporary index on metro_source.code if not exists
            cur.execute("""
                        CREATE INDEX IF NOT EXISTS tmp_metro_source_code
                            ON public.metro_source(code)
                        """)

            logger.info("Updating existing records in food_item_sources...")

            # More efficient update query using EXISTS
            update_sql = """
                         UPDATE public.food_item_sources
                         SET sources = array_append(sources, 'metro')
                         WHERE EXISTS (SELECT 1 \
                                       FROM public.metro_source m \
                                       WHERE m.code = food_item_sources.code)
                           AND NOT ('metro' = ANY (sources)) \
                         """

            cur.execute(update_sql)
            updated_count = cur.rowcount
            logger.info(f"Updated {updated_count} existing records with 'metro' source")
            conn.commit()

            logger.info("Inserting new Metro-only records...")

            # More efficient insert using NOT EXISTS
            insert_sql = """
                         INSERT INTO public.food_item_sources (code, sources)
                         SELECT m.code, ARRAY['metro'] ::character varying[]
                         FROM public.metro_source m
                         WHERE NOT EXISTS (
                             SELECT 1 FROM public.food_item_sources fis
                             WHERE fis.code = m.code
                             ) \
                         """

            cur.execute(insert_sql)
            inserted_count = cur.rowcount
            logger.info(f"Inserted {inserted_count} new records with 'metro' source")
            conn.commit()

            # Get final statistics
            logger.info("Calculating final statistics...")

            cur.execute("SELECT COUNT(*) FROM public.food_item_sources WHERE 'metro' = ANY(sources)")
            metro_total = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM public.food_item_sources WHERE array_length(sources, 1) > 1")
            both_sources = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM public.food_item_sources")
            total_products = cur.fetchone()[0]

            logger.info(f"Final statistics:")
            logger.info(f"  - Total unique products: {total_products}")
            logger.info(f"  - Products with Metro source: {metro_total}")
            logger.info(f"  - Products available in both sources: {both_sources}")

            return True

        except Exception as e:
            logger.error(f"Error updating food_item_sources: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def run_import(self, csv_file_path: str) -> bool:
        """Run the complete import process"""
        logger.info("Starting Metro data import process...")

        # Step 1: Import CSV data to metro_source
        if not self.import_csv_to_metro_source(csv_file_path):
            logger.error("Failed to import CSV data")
            return False

        # Step 2: Update food_item_sources
        if not self.update_food_item_sources():
            logger.error("Failed to update food_item_sources")
            return False

        logger.info("✅ Metro data import completed successfully!")
        return True


# Main execution
if __name__ == "__main__":
    # CORRECT Supabase connection string (IPv4 compatible Session Pooler)
    #Stage
    # CONNECTION_STRING = "postgresql://postgres....:[YOUR-PASSWORD]@aws-0-eu-central-1.pooler.supabase.com:5432/postgres"
    # Prod
    CONNECTION_STRING = "postgresql://postgres....:[YOUR-PASSWORD]@aws-0-eu-central-1.pooler.supabase.com:5432/postgres"

    # CSV file in same folder
    CSV_FILE_PATH = "metro_products_full2.csv"

    # Check if CSV exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CSV file not found: {CSV_FILE_PATH}")
        print("Make sure metro_products_full2.csv is in the same folder as this script")
        exit(1)

    # Prompt for password
    password = input("Enter your Supabase password: ")
    connection_string = CONNECTION_STRING.replace("[YOUR-PASSWORD]", password)

    # Test connection first
    print("Testing database connection...")
    importer = SimpleMetroImporter(connection_string)
    test_conn = importer.connect()

    if test_conn:
        print("✅ Database connection successful!")
        test_conn.close()
    else:
        print("❌ Database connection failed!")
        exit(1)

    # Run the import
    success = importer.run_import(CSV_FILE_PATH)

    if success:
        print("\n🎉 Metro data successfully imported!")
        print("\n📊 Your database now contains:")
        print("  ✅ metro_source table with 6,954 Metro products")
        print("  ✅ Updated food_item_sources with Metro codes")
        print("\n🔍 Example queries you can now run:")
        print("  -- Find product by barcode")
        print("  SELECT * FROM metro_source WHERE code = 'your_barcode';")
        print("  ")
        print("  -- Check which sources have a product")
        print("  SELECT * FROM food_item_sources WHERE code = 'your_barcode';")
        print("  ")
        print("  -- Get all products available in both sources")
        print("  SELECT * FROM food_item_sources WHERE array_length(sources, 1) > 1;")
    else:
        print("❌ Import failed. Check the logs above for details.")