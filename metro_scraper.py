import requests
import json
import time
import pandas as pd
import logging
from typing import List, Dict, Optional, Set
from urllib.parse import quote


class MetroProductScraper:
    def __init__(self, delay: float = 1.5):
        self.base_url = "https://shop.metro.bg"
        self.delay = delay

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'bg-BG,bg;q=0.9,en;q=0.8',
            'Referer': 'https://shop.metro.bg/'
        })

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('metro_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _make_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with error handling and rate limiting"""
        try:
            time.sleep(self.delay)
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error for {url}: {e}")
            return None

    def convert_variant_to_article_id(self, variant_id: str) -> str:
        """Convert variant ID (BTY-X2945500032) to article ID (BTY-X294550)"""
        if len(variant_id) > 4 and variant_id[-4:].isdigit():
            return variant_id[:-4]
        return variant_id

    def get_food_subcategories(self) -> List[str]:
        """Get all food subcategory paths to bypass pagination limits"""
        self.logger.info("Fetching food subcategories...")

        # Get the main food category page to extract subcategories
        params = {
            'storeId': '00010',
            'language': 'bg-BG',
            'country': 'BG',
            'query': '*',
            'rows': 1,
            'page': 1,
            'filter': 'category:хранителни-стоки',
            'facets': 'true',
            'categories': 'true',
            '__t': int(time.time() * 1000)
        }

        url = f"{self.base_url}/searchdiscover/articlesearch/search"
        response = self._make_request(url, params)

        categories = []

        if response and 'categorytree' in response:
            def extract_categories(tree_node, current_path=""):
                """Recursively extract category paths"""
                if 'children' in tree_node:
                    for category_id, category_data in tree_node['children'].items():
                        category_path = category_data.get('urlCategoryPath', '')
                        if category_path and category_path.startswith('хранителни-стоки'):
                            categories.append(category_path)
                            # Recursively process children
                            extract_categories(category_data, category_path)

            # Start with the food category tree
            food_tree = response['categorytree']['children'].get('Food_1622788118100', {})
            extract_categories(food_tree)

        # If we couldn't extract subcategories, fall back to main category
        if not categories:
            categories = ['хранителни-стоки']

        self.logger.info(f"Found {len(categories)} food categories to scrape")
        return categories

    def get_product_variant_ids_from_category(self, category: str) -> Set[str]:
        """Get all product variant IDs from a specific category"""
        self.logger.info(f"Fetching products from category: {category}")

        all_variant_ids = set()
        page = 1

        while True:
            params = {
                'storeId': '00010',
                'language': 'bg-BG',
                'country': 'BG',
                'query': '*',
                'rows': 100,  # Use larger page size for efficiency since we're doing fewer categories
                'page': page,
                'filter': f'category:{category}',
                'facets': 'true',
                'categories': 'true',
                '__t': int(time.time() * 1000)
            }

            url = f"{self.base_url}/searchdiscover/articlesearch/search"
            response = self._make_request(url, params)

            if not response or 'resultIds' not in response:
                self.logger.warning(f"Failed to get results for category {category}, page {page}")
                break

            result_ids = response['resultIds']
            if not result_ids:
                break

            all_variant_ids.update(result_ids)
            self.logger.info(
                f"Category {category}, page {page}: Found {len(result_ids)} products (Category total: {len(all_variant_ids)})")

            # Check if we've reached the last page or hit pagination limit
            if page >= response.get('totalPages', 1) or page >= 100:  # Safety limit
                break

            page += 1

        return all_variant_ids

    def get_all_product_variant_ids(self) -> Set[str]:
        """Get all food product variant IDs from Metro by scraping each subcategory"""
        self.logger.info("Starting category-based scraping to get all products...")

        # Get all food subcategories
        categories = self.get_food_subcategories()

        all_variant_ids = set()

        for i, category in enumerate(categories, 1):
            self.logger.info(f"Processing category {i}/{len(categories)}: {category}")

            try:
                category_ids = self.get_product_variant_ids_from_category(category)
                all_variant_ids.update(category_ids)
                self.logger.info(
                    f"Category {category}: {len(category_ids)} products (Total unique: {len(all_variant_ids)})")

                # Add delay between categories
                time.sleep(2)

            except Exception as e:
                self.logger.error(f"Error processing category {category}: {e}")
                continue

        self.logger.info(f"Finished fetching IDs from all categories. Total unique variant IDs: {len(all_variant_ids)}")
        return all_variant_ids

    def get_product_details_batch(self, article_ids: List[str]) -> Optional[Dict]:
        """Get product details for multiple article IDs in one request"""
        url = f"{self.base_url}/evaluate.article.v1/betty-articles"

        # Build URL with multiple IDs
        params = {
            'country': 'BG',
            'locale': 'bg-BG',
            'storeIds': '00010',
            'details': 'true',
            '__t': int(time.time() * 1000)
        }

        # Add multiple IDs as separate parameters
        param_pairs = []
        for key, value in params.items():
            param_pairs.append(f'{key}={value}')

        for article_id in article_ids:
            param_pairs.append(f'ids={article_id}')

        full_url = f"{url}?{'&'.join(param_pairs)}"

        try:
            time.sleep(self.delay)
            response = self.session.get(full_url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Error getting product details batch: {e}")
            return None

    def extract_nutritional_value(self, nutrition_table: Dict, label_keywords: List[str], unit: str = None) -> Optional[
        float]:
        """Extract nutritional value from Metro's nutrition table"""
        if not nutrition_table or 'rows' not in nutrition_table:
            return None

        for row in nutrition_table['rows']:
            row_label = row.get('rowLabel', '').lower()
            cells = row.get('cells', [])

            # Check if this row matches our keywords
            if any(keyword in row_label for keyword in label_keywords):
                if len(cells) >= 1 and cells[0].get('value'):
                    try:
                        value = float(cells[0].get('value', ''))
                        cell_unit = cells[0].get('unitOfMeasure', '').lower()

                        # Unit conversion if needed
                        if unit and unit.lower() != cell_unit:
                            if unit.lower() == 'mg' and cell_unit == 'g':
                                value = value * 1000  # Convert grams to milligrams

                        return value
                    except (ValueError, TypeError):
                        continue
        return None

    def extract_ingredients(self, features: List[Dict]) -> Optional[str]:
        """Extract ingredients from product features"""
        for feature in features:
            if feature.get('featureType') == 'ingredientStatement':
                leafs = feature.get('leafs', [])
                ingredient_parts = []

                for leaf in leafs:
                    meta_info = leaf.get('metaInfo', '')
                    if meta_info in ['Contains', ''] and leaf.get('label'):
                        label = leaf.get('label', '').strip()
                        if label and label not in ['INGREDIENTS', '(', ')']:
                            ingredient_parts.append(label)

                if ingredient_parts:
                    return ' '.join(ingredient_parts)
        return None

    def extract_product_data(self, article_data: Dict) -> Optional[Dict]:
        """Extract product data matching the OpenFoodFacts schema"""
        try:
            # Navigate Metro's nested structure
            variants = article_data.get('variants', {})
            if not variants:
                return None

            # Get first variant
            variant = list(variants.values())[0]
            bundles = variant.get('bundles', {})
            if not bundles:
                return None

            # Get first bundle
            bundle = list(bundles.values())[0]

            # Extract barcode (most important for your use case)
            ean_numbers = bundle.get('eanNumber', []) or bundle.get('gtins', [])
            code = None
            if ean_numbers and len(ean_numbers) > 0:
                code = ean_numbers[0].get('number')

            if not code:
                return None  # Skip products without barcodes

            # Extract basic info
            product_name = bundle.get('description', '')
            brand = bundle.get('brandName', '')

            # Extract quantity
            content_data = bundle.get('contentData', {})
            net_weight = content_data.get('netPieceWeight', {})
            quantity = None
            if net_weight and net_weight.get('value'):
                value = net_weight.get('value', '')
                unit = net_weight.get('uom', '')
                quantity = f"{value} {unit}".strip()

            # Extract categories
            categories = bundle.get('categories', [])
            category_parts = []
            for cat in categories:
                levels = cat.get('levels', [])
                if levels:
                    level_names = [level.get('displayName', '') for level in levels if level.get('displayName')]
                    if level_names:
                        category_parts.append(' > '.join(level_names))

            categories_str = ' | '.join(category_parts) if category_parts else None

            # Extract image URL
            image_url = bundle.get('imageUrl') or bundle.get('imageUrlL')

            # Extract detailed info
            details = bundle.get('details', {})
            ingredients = None
            nutrition_data = {}

            if details:
                # Extract ingredients
                features = details.get('features', [])
                ingredients = self.extract_ingredients(features)

                # Extract nutrition
                nutrition_table = details.get('nutritionalTable', {})
                if nutrition_table:
                    nutrition_data = {
                        'energy_100g': self.extract_nutritional_value(nutrition_table, ['енергийна стойност'], 'kcal'),
                        'fat_100g': self.extract_nutritional_value(nutrition_table, ['мазнини'], 'g'),
                        'saturated_fat_100g': self.extract_nutritional_value(nutrition_table, ['наситени'], 'g'),
                        'proteins_100g': self.extract_nutritional_value(nutrition_table, ['белтъци'], 'g'),
                        'carbohydrates_100g': self.extract_nutritional_value(nutrition_table, ['въглехидрати'], 'g'),
                        'sugars_100g': self.extract_nutritional_value(nutrition_table, ['захари'], 'g'),
                        'fiber_100g': self.extract_nutritional_value(nutrition_table, ['влакна', 'fiber'], 'g'),
                        'sodium_100g': self.extract_nutritional_value(nutrition_table, ['sodium'], 'mg')
                    }

                    # Special handling for energy in kJ (convert to kcal if needed)
                    if not nutrition_data.get('energy_100g'):
                        energy_kj = self.extract_nutritional_value(nutrition_table, ['енергийна стойност'], 'kJ')
                        if energy_kj:
                            nutrition_data['energy_100g'] = round(energy_kj / 4.184, 1)  # Convert kJ to kcal

            return {
                'code': code,
                'product_name': product_name or None,
                'quantity': quantity,
                'brand': brand or None,
                'categories': categories_str,
                'ingredients': ingredients,
                'image_url': image_url,
                'nutriscore_grade': None,  # Metro doesn't have nutriscore
                'energy_100g': nutrition_data.get('energy_100g'),
                'fat_100g': nutrition_data.get('fat_100g'),
                'saturated_fat_100g': nutrition_data.get('saturated_fat_100g'),
                'proteins_100g': nutrition_data.get('proteins_100g'),
                'carbohydrates_100g': nutrition_data.get('carbohydrates_100g'),
                'sugars_100g': nutrition_data.get('sugars_100g'),
                'fiber_100g': nutrition_data.get('fiber_100g'),
                'sodium_100g': nutrition_data.get('sodium_100g')
            }

        except Exception as e:
            self.logger.error(f"Error extracting product data: {e}")
            return None

    def scrape_all_products(self) -> List[Dict]:
        """Main method to scrape all Metro food products"""
        self.logger.info("Starting Metro product scraping...")

        # Step 1: Get all variant IDs
        variant_ids = self.get_all_product_variant_ids()
        if not variant_ids:
            self.logger.error("No product IDs found")
            return []

        # Step 2: Convert to article IDs and remove duplicates
        article_ids = list(set(self.convert_variant_to_article_id(vid) for vid in variant_ids))
        self.logger.info(f"Converted to {len(article_ids)} unique article IDs")

        # Step 3: Fetch product details in batches
        all_products = []
        batch_size = 20  # Metro API can handle multiple IDs per request
        total_batches = (len(article_ids) + batch_size - 1) // batch_size

        for i in range(0, len(article_ids), batch_size):
            batch_num = i // batch_size + 1
            batch_ids = article_ids[i:i + batch_size]

            self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_ids)} products)")

            # Get product details
            batch_response = self.get_product_details_batch(batch_ids)

            if batch_response and 'result' in batch_response:
                for article_id, article_data in batch_response['result'].items():
                    product_data = self.extract_product_data(article_data)
                    if product_data:
                        all_products.append(product_data)
                        if len(all_products) % 100 == 0:
                            self.logger.info(f"Processed {len(all_products)} products so far...")
            else:
                self.logger.warning(f"Failed to get data for batch {batch_num}")

        self.logger.info(f"Scraping completed! Total products with barcodes: {len(all_products)}")
        return all_products

    def save_to_csv(self, products: List[Dict], filename: str = "metro_products.csv"):
        """Save products to CSV file"""
        if not products:
            self.logger.warning("No products to save")
            return

        # Create DataFrame with exact schema columns
        df = pd.DataFrame(products)

        # Ensure all required columns exist
        required_columns = [
            'code', 'product_name', 'quantity', 'brand', 'categories',
            'ingredients', 'image_url', 'nutriscore_grade', 'energy_100g',
            'fat_100g', 'saturated_fat_100g', 'proteins_100g',
            'carbohydrates_100g', 'sugars_100g', 'fiber_100g', 'sodium_100g'
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        # Reorder columns to match schema
        df = df[required_columns]

        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        self.logger.info(f"Saved {len(products)} products to {filename}")

        # Print some statistics
        self.logger.info(f"Products with nutritional data: {df['energy_100g'].notna().sum()}")
        self.logger.info(f"Products with ingredients: {df['ingredients'].notna().sum()}")
        self.logger.info(f"Products with images: {df['image_url'].notna().sum()}")


# Main execution
if __name__ == "__main__":
    scraper = MetroProductScraper(delay=2.0)  # 2 second delay to be respectful

    # Test with a small batch first (comment out to skip test)
    # print("Testing with small batch...")
    # test_ids = ['BTY-X294550', 'BTY-X334084']
    # test_response = scraper.get_product_details_batch(test_ids)
    # if test_response:
    #     test_products = []
    #     for article_id, article_data in test_response['result'].items():
    #         product_data = scraper.extract_product_data(article_data)
    #         if product_data:
    #             test_products.append(product_data)
    #     scraper.save_to_csv(test_products, "metro_test.csv")
    #     print(f"Test completed: {len(test_products)} products")

    # Run full scrape with smaller page size to get all products
    print("Starting full Metro scrape with page size 24...")
    products = scraper.scrape_all_products()

    if products:
        scraper.save_to_csv(products, "metro_products_full2.csv")
        print(f"\nScraping completed successfully!")
        print(f"Total products: {len(products)}")
        print(f"Data saved to: metro_products_full2.csv")
        print(f"Previous backup preserved in: metro_products_full.csv")
    else:
        print("Scraping failed - no products found")