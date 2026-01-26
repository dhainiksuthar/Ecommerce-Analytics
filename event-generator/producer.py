"""
E-Commerce Event Generator
==========================
Generates realistic clickstream, order, and inventory events with:
- Proper relationships between entities (users, products, orders)
- Realistic user behavior patterns (browsing → cart → purchase funnel)
- Data quality issues (nulls, duplicates, late events, invalid data)
- Time-based patterns (peak hours, weekends)
- Geographic distribution

Author: Dhainik Suthar
Project: Ecommerce-Analytics
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from faker import Faker
from kafka import KafkaProducer

# =============================================================================
# CONFIGURATION
# =============================================================================

fake = Faker()
Faker.seed(42)
random.seed(42)

KAFKA_BOOTSTRAP_SERVERS = ['kafka:9093']

# Topic names
CLICKSTREAM_TOPIC = "clickstream"
ORDERS_TOPIC = "orders"
INVENTORY_TOPIC = "inventory"

# =============================================================================
# REFERENCE DATA (Dimensions)
# =============================================================================

# Product Catalog - realistic e-commerce products
PRODUCTS = [
    {"product_id": "PROD-001", "name": "iPhone 15 Pro Max", "category": "Electronics", "subcategory": "Smartphones", "brand": "Apple", "base_price": 1199.00, "cost": 850.00, "weight_kg": 0.22, "inventory_qty": 500},
    {"product_id": "PROD-002", "name": "Samsung Galaxy S24 Ultra", "category": "Electronics", "subcategory": "Smartphones", "brand": "Samsung", "base_price": 1099.00, "cost": 750.00, "weight_kg": 0.23, "inventory_qty": 450},
    {"product_id": "PROD-003", "name": "MacBook Pro 14\"", "category": "Electronics", "subcategory": "Laptops", "brand": "Apple", "base_price": 1999.00, "cost": 1400.00, "weight_kg": 1.6, "inventory_qty": 200},
    {"product_id": "PROD-004", "name": "Dell XPS 15", "category": "Electronics", "subcategory": "Laptops", "brand": "Dell", "base_price": 1499.00, "cost": 1000.00, "weight_kg": 1.8, "inventory_qty": 180},
    {"product_id": "PROD-005", "name": "Sony WH-1000XM5", "category": "Electronics", "subcategory": "Headphones", "brand": "Sony", "base_price": 349.00, "cost": 200.00, "weight_kg": 0.25, "inventory_qty": 800},
    {"product_id": "PROD-006", "name": "AirPods Pro 2", "category": "Electronics", "subcategory": "Headphones", "brand": "Apple", "base_price": 249.00, "cost": 150.00, "weight_kg": 0.05, "inventory_qty": 1000},
    {"product_id": "PROD-007", "name": "Nike Air Max 90", "category": "Footwear", "subcategory": "Sneakers", "brand": "Nike", "base_price": 130.00, "cost": 60.00, "weight_kg": 0.8, "inventory_qty": 600},
    {"product_id": "PROD-008", "name": "Adidas Ultraboost 23", "category": "Footwear", "subcategory": "Running Shoes", "brand": "Adidas", "base_price": 190.00, "cost": 90.00, "weight_kg": 0.7, "inventory_qty": 550},
    {"product_id": "PROD-009", "name": "Levi's 501 Original Jeans", "category": "Apparel", "subcategory": "Jeans", "brand": "Levi's", "base_price": 69.50, "cost": 25.00, "weight_kg": 0.6, "inventory_qty": 1200},
    {"product_id": "PROD-010", "name": "North Face Puffer Jacket", "category": "Apparel", "subcategory": "Jackets", "brand": "The North Face", "base_price": 299.00, "cost": 120.00, "weight_kg": 0.9, "inventory_qty": 300},
    {"product_id": "PROD-011", "name": "Instant Pot Duo 7-in-1", "category": "Home & Kitchen", "subcategory": "Appliances", "brand": "Instant Pot", "base_price": 89.99, "cost": 45.00, "weight_kg": 5.5, "inventory_qty": 400},
    {"product_id": "PROD-012", "name": "Dyson V15 Detect", "category": "Home & Kitchen", "subcategory": "Vacuum Cleaners", "brand": "Dyson", "base_price": 749.00, "cost": 400.00, "weight_kg": 3.1, "inventory_qty": 150},
    {"product_id": "PROD-013", "name": "Kindle Paperwhite", "category": "Electronics", "subcategory": "E-Readers", "brand": "Amazon", "base_price": 139.99, "cost": 80.00, "weight_kg": 0.2, "inventory_qty": 700},
    {"product_id": "PROD-014", "name": "Fitbit Charge 6", "category": "Electronics", "subcategory": "Wearables", "brand": "Fitbit", "base_price": 159.95, "cost": 90.00, "weight_kg": 0.03, "inventory_qty": 900},
    {"product_id": "PROD-015", "name": "LEGO Star Wars Millennium Falcon", "category": "Toys & Games", "subcategory": "Building Sets", "brand": "LEGO", "base_price": 169.99, "cost": 80.00, "weight_kg": 2.5, "inventory_qty": 250},
    {"product_id": "PROD-016", "name": "PlayStation 5", "category": "Electronics", "subcategory": "Gaming Consoles", "brand": "Sony", "base_price": 499.00, "cost": 380.00, "weight_kg": 4.5, "inventory_qty": 100},
    {"product_id": "PROD-017", "name": "Nintendo Switch OLED", "category": "Electronics", "subcategory": "Gaming Consoles", "brand": "Nintendo", "base_price": 349.00, "cost": 250.00, "weight_kg": 0.42, "inventory_qty": 350},
    {"product_id": "PROD-018", "name": "Yeti Rambler 30oz", "category": "Sports & Outdoors", "subcategory": "Drinkware", "brand": "Yeti", "base_price": 38.00, "cost": 15.00, "weight_kg": 0.5, "inventory_qty": 2000},
    {"product_id": "PROD-019", "name": "Patagonia Better Sweater", "category": "Apparel", "subcategory": "Fleece", "brand": "Patagonia", "base_price": 139.00, "cost": 55.00, "weight_kg": 0.5, "inventory_qty": 400},
    {"product_id": "PROD-020", "name": "Bose SoundLink Revolve+", "category": "Electronics", "subcategory": "Speakers", "brand": "Bose", "base_price": 329.00, "cost": 180.00, "weight_kg": 0.9, "inventory_qty": 300},
]

# Geographic distribution (weighted by e-commerce activity)
COUNTRIES = [
    {"code": "US", "name": "United States", "weight": 35, "currency": "USD", "timezone": "America/New_York"},
    {"code": "GB", "name": "United Kingdom", "weight": 12, "currency": "GBP", "timezone": "Europe/London"},
    {"code": "DE", "name": "Germany", "weight": 10, "currency": "EUR", "timezone": "Europe/Berlin"},
    {"code": "FR", "name": "France", "weight": 8, "currency": "EUR", "timezone": "Europe/Paris"},
    {"code": "IN", "name": "India", "weight": 15, "currency": "INR", "timezone": "Asia/Kolkata"},
    {"code": "CA", "name": "Canada", "weight": 6, "currency": "CAD", "timezone": "America/Toronto"},
    {"code": "AU", "name": "Australia", "weight": 5, "currency": "AUD", "timezone": "Australia/Sydney"},
    {"code": "JP", "name": "Japan", "weight": 5, "currency": "JPY", "timezone": "Asia/Tokyo"},
    {"code": "BR", "name": "Brazil", "weight": 4, "currency": "BRL", "timezone": "America/Sao_Paulo"},
]

# Traffic sources
TRAFFIC_SOURCES = [
    {"source": "google", "medium": "organic", "weight": 30},
    {"source": "google", "medium": "cpc", "weight": 20},
    {"source": "facebook", "medium": "social", "weight": 15},
    {"source": "instagram", "medium": "social", "weight": 10},
    {"source": "direct", "medium": "none", "weight": 12},
    {"source": "email", "medium": "newsletter", "weight": 8},
    {"source": "bing", "medium": "organic", "weight": 3},
    {"source": "twitter", "medium": "social", "weight": 2},
]

# Device and browser combinations
DEVICES = [
    {"type": "mobile", "os": "iOS", "browser": "Safari", "weight": 25},
    {"type": "mobile", "os": "Android", "browser": "Chrome", "weight": 30},
    {"type": "desktop", "os": "Windows", "browser": "Chrome", "weight": 20},
    {"type": "desktop", "os": "macOS", "browser": "Safari", "weight": 10},
    {"type": "desktop", "os": "Windows", "browser": "Edge", "weight": 8},
    {"type": "tablet", "os": "iOS", "browser": "Safari", "weight": 5},
    {"type": "desktop", "os": "Linux", "browser": "Firefox", "weight": 2},
]

# Page paths
PAGES = {
    "home": "/",
    "search": "/search",
    "category": "/category/{category}",
    "product": "/product/{product_id}",
    "cart": "/cart",
    "checkout": "/checkout",
    "checkout_shipping": "/checkout/shipping",
    "checkout_payment": "/checkout/payment",
    "checkout_confirm": "/checkout/confirm",
    "order_confirmation": "/order/confirmation/{order_id}",
    "account": "/account",
    "login": "/login",
    "signup": "/signup",
    "help": "/help",
    "returns": "/returns",
}

# Search queries (realistic e-commerce searches)
SEARCH_QUERIES = [
    "iphone 15", "laptop deals", "wireless headphones", "running shoes",
    "winter jacket", "gaming console", "smart watch", "bluetooth speaker",
    "vacuum cleaner", "coffee maker", "men's jeans", "yoga mat",
    "backpack", "sunglasses", "protein powder", "desk chair",
    "monitor", "keyboard", "mouse", "webcam", "microphone",
    "air fryer", "blender", "toaster", "electric kettle",
    None, None, None,  # Some searches have typos or are empty
    "iphon", "lapto", "headphons",  # Typos
]

# Payment methods
PAYMENT_METHODS = [
    {"method": "credit_card", "provider": "Visa", "weight": 35},
    {"method": "credit_card", "provider": "Mastercard", "weight": 25},
    {"method": "credit_card", "provider": "Amex", "weight": 10},
    {"method": "paypal", "provider": "PayPal", "weight": 15},
    {"method": "apple_pay", "provider": "Apple", "weight": 8},
    {"method": "google_pay", "provider": "Google", "weight": 5},
    {"method": "buy_now_pay_later", "provider": "Klarna", "weight": 2},
]

# Order statuses
ORDER_STATUSES = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"]

# Shipping methods
SHIPPING_METHODS = [
    {"method": "standard", "days": 5, "cost": 5.99, "weight": 50},
    {"method": "express", "days": 2, "cost": 12.99, "weight": 30},
    {"method": "next_day", "days": 1, "cost": 24.99, "weight": 15},
    {"method": "free", "days": 7, "cost": 0.00, "weight": 5},  # Orders over threshold
]

# Coupon codes
COUPON_CODES = [
    {"code": "SAVE10", "discount_type": "percentage", "discount_value": 10, "min_order": 50},
    {"code": "SAVE20", "discount_type": "percentage", "discount_value": 20, "min_order": 100},
    {"code": "FLAT15", "discount_type": "fixed", "discount_value": 15, "min_order": 75},
    {"code": "FREESHIP", "discount_type": "free_shipping", "discount_value": 0, "min_order": 0},
    {"code": "WELCOME25", "discount_type": "percentage", "discount_value": 25, "min_order": 0},
    None,  # No coupon
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def weighted_choice(items: List[Dict], weight_key: str = "weight") -> Dict:
    """Select item based on weight."""
    weights = [item[weight_key] for item in items]
    return random.choices(items, weights=weights, k=1)[0]


def generate_uuid() -> str:
    """Generate a UUID string."""
    return str(uuid.uuid4())


def get_current_hour_weight() -> float:
    """
    Return a weight multiplier based on current hour.
    Simulates realistic traffic patterns (more traffic in evening).
    """
    hour = datetime.now().hour
    
    # Traffic pattern: low at night, peak in evening
    hour_weights = {
        0: 0.3, 1: 0.2, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.2,
        6: 0.4, 7: 0.6, 8: 0.8, 9: 1.0, 10: 1.1, 11: 1.2,
        12: 1.3, 13: 1.2, 14: 1.1, 15: 1.0, 16: 1.1, 17: 1.3,
        18: 1.5, 19: 1.7, 20: 1.8, 21: 1.6, 22: 1.2, 23: 0.7
    }
    return hour_weights.get(hour, 1.0)


def maybe_null(value, null_probability: float = 0.05):
    """Return None with given probability, otherwise return value."""
    if random.random() < null_probability:
        return None
    return value


def maybe_corrupt(value: str, corrupt_probability: float = 0.02) -> str:
    """Occasionally corrupt string data (realistic data quality issues)."""
    if random.random() < corrupt_probability:
        corruptions = [
            lambda v: v.upper(),  # Wrong case
            lambda v: v + " ",    # Trailing space
            lambda v: " " + v,    # Leading space
            lambda v: v.replace("-", ""),  # Missing delimiter
            lambda v: "",         # Empty string
        ]
        return random.choice(corruptions)(value)
    return value


def generate_late_timestamp(base_time: datetime, late_probability: float = 0.1) -> Tuple[datetime, bool]:
    """
    Generate timestamp that might be late (for testing late-arriving data handling).
    Returns (timestamp, is_late).
    """
    if random.random() < late_probability:
        # Late by 1-60 minutes
        delay = timedelta(minutes=random.randint(1, 60))
        return base_time - delay, True
    return base_time, False


# =============================================================================
# USER SESSION MANAGEMENT
# =============================================================================

class UserSessionManager:
    """Manages user sessions with realistic behavior patterns."""
    
    def __init__(self):
        self.active_sessions: Dict[str, Dict] = {}
        self.registered_users: Dict[str, Dict] = {}
        self.session_carts: Dict[str, List[Dict]] = {}  # session_id -> cart items
        
    def create_user(self, is_registered: bool = True) -> Dict:
        """Create a new user (registered or anonymous)."""
        user_id = generate_uuid()
        country = weighted_choice(COUNTRIES)
        
        user = {
            "user_id": user_id,
            "is_anonymous": not is_registered,
            "email": fake.email() if is_registered else None,
            "first_name": fake.first_name() if is_registered else None,
            "last_name": fake.last_name() if is_registered else None,
            "phone": fake.phone_number() if is_registered and random.random() > 0.3 else None,
            "country_code": country["code"],
            "country_name": country["name"],
            "city": fake.city(),
            "postal_code": fake.postcode() if is_registered else None,
            "registration_date": fake.date_time_between(start_date='-2y', end_date='-1d').isoformat() if is_registered else None,
            "loyalty_tier": random.choice(["bronze", "silver", "gold", "platinum"]) if is_registered else None,
            "lifetime_value": round(random.uniform(0, 5000), 2) if is_registered else 0,
        }
        
        if is_registered:
            self.registered_users[user_id] = user
            
        return user
    
    def create_session(self, user: Dict) -> Dict:
        """Create a new session for a user."""
        device = weighted_choice(DEVICES)
        traffic = weighted_choice(TRAFFIC_SOURCES)
        
        session_id = generate_uuid()
        session = {
            "session_id": session_id,
            "user_id": user["user_id"],
            "started_at": datetime.now().isoformat(),
            "device_type": device["type"],
            "device_os": device["os"],
            "browser": device["browser"],
            "browser_version": f"{random.randint(90, 120)}.0.{random.randint(1000, 9999)}.{random.randint(10, 99)}",
            "screen_resolution": random.choice(["1920x1080", "1366x768", "2560x1440", "1440x900", "375x812", "414x896"]),
            "traffic_source": traffic["source"],
            "traffic_medium": traffic["medium"],
            "campaign": f"campaign_{random.randint(1, 20)}" if traffic["medium"] == "cpc" else None,
            "country_code": user["country_code"],
            "city": user["city"],
            "ip_address": fake.ipv4(),
            "user_agent": fake.user_agent(),
            "is_new_visitor": user.get("is_anonymous", True) or random.random() > 0.6,
            "landing_page": random.choice(["/", "/search", "/category/electronics", "/product/PROD-001"]),
        }
        
        self.active_sessions[session_id] = {
            "session": session,
            "user": user,
            "page_views": 0,
            "events": [],
            "funnel_stage": "browsing",  # browsing -> considering -> cart -> checkout -> purchased
        }
        self.session_carts[session_id] = []
        
        return session
    
    def get_or_create_session(self) -> Tuple[Dict, Dict]:
        """Get existing session or create new one."""
        # 70% chance to continue existing session
        if self.active_sessions and random.random() < 0.7:
            session_id = random.choice(list(self.active_sessions.keys()))
            session_data = self.active_sessions[session_id]
            return session_data["session"], session_data["user"]
        
        # Create new session
        is_registered = random.random() > 0.4  # 60% registered users
        
        # 30% chance returning registered user
        if is_registered and self.registered_users and random.random() < 0.3:
            user = random.choice(list(self.registered_users.values()))
        else:
            user = self.create_user(is_registered)
            
        session = self.create_session(user)
        return session, user
    
    def update_session_stage(self, session_id: str, stage: str):
        """Update the funnel stage of a session."""
        if session_id in self.active_sessions:
            self.active_sessions[session_id]["funnel_stage"] = stage
    
    def add_to_cart(self, session_id: str, product: Dict, quantity: int = 1):
        """Add item to session cart."""
        if session_id not in self.session_carts:
            self.session_carts[session_id] = []
        
        # Check if product already in cart
        for item in self.session_carts[session_id]:
            if item["product_id"] == product["product_id"]:
                item["quantity"] += quantity
                return
        
        self.session_carts[session_id].append({
            "product_id": product["product_id"],
            "product_name": product["name"],
            "price": product["base_price"],
            "quantity": quantity,
        })
    
    def get_cart(self, session_id: str) -> List[Dict]:
        """Get cart contents for session."""
        return self.session_carts.get(session_id, [])
    
    def clear_cart(self, session_id: str):
        """Clear cart after purchase."""
        if session_id in self.session_carts:
            self.session_carts[session_id] = []
    
    def end_session(self, session_id: str):
        """End a session (user leaves site)."""
        if session_id in self.active_sessions:
            del self.active_sessions[session_id]
        if session_id in self.session_carts:
            del self.session_carts[session_id]
    
    def cleanup_old_sessions(self, max_age_minutes: int = 30):
        """Remove sessions older than max_age."""
        # In production, would check timestamps
        if len(self.active_sessions) > 100:
            # Remove oldest 20%
            sessions_to_remove = list(self.active_sessions.keys())[:20]
            for session_id in sessions_to_remove:
                self.end_session(session_id)


# =============================================================================
# INVENTORY MANAGER
# =============================================================================

class InventoryManager:
    """Manages product inventory with realistic patterns."""
    
    def __init__(self):
        self.inventory: Dict[str, int] = {p["product_id"]: p["inventory_qty"] for p in PRODUCTS}
        self.reserved: Dict[str, int] = {p["product_id"]: 0 for p in PRODUCTS}
    
    def get_available_quantity(self, product_id: str) -> int:
        """Get available (unreserved) quantity."""
        return self.inventory.get(product_id, 0) - self.reserved.get(product_id, 0)
    
    def reserve_stock(self, product_id: str, quantity: int) -> bool:
        """Reserve stock for an order."""
        available = self.get_available_quantity(product_id)
        if available >= quantity:
            self.reserved[product_id] = self.reserved.get(product_id, 0) + quantity
            return True
        return False
    
    def confirm_sale(self, product_id: str, quantity: int):
        """Confirm sale - reduce inventory and reserved."""
        self.inventory[product_id] = self.inventory.get(product_id, 0) - quantity
        self.reserved[product_id] = max(0, self.reserved.get(product_id, 0) - quantity)
    
    def cancel_reservation(self, product_id: str, quantity: int):
        """Cancel reservation - release reserved stock."""
        self.reserved[product_id] = max(0, self.reserved.get(product_id, 0) - quantity)
    
    def restock(self, product_id: str, quantity: int):
        """Add stock (shipment received)."""
        self.inventory[product_id] = self.inventory.get(product_id, 0) + quantity
    
    def generate_inventory_event(self, event_type: str, product_id: str, 
                                  quantity: int, reason: str = None) -> Dict:
        """Generate an inventory change event."""
        product = next((p for p in PRODUCTS if p["product_id"] == product_id), None)
        
        timestamp, is_late = generate_late_timestamp(datetime.now())
        
        return {
            "event_id": generate_uuid(),
            "event_type": event_type,  # stock_update, reservation, sale, restock, adjustment
            "event_timestamp": timestamp.isoformat(),
            "is_late_arriving": is_late,
            "product_id": product_id,
            "product_name": product["name"] if product else None,
            "category": product["category"] if product else None,
            "quantity_change": quantity,
            "quantity_before": self.inventory.get(product_id, 0),
            "quantity_after": self.inventory.get(product_id, 0) + quantity if event_type == "restock" else self.inventory.get(product_id, 0) - quantity,
            "reserved_quantity": self.reserved.get(product_id, 0),
            "reason": reason,
            "warehouse_id": random.choice(["WH-001", "WH-002", "WH-003"]),
            "warehouse_location": random.choice(["New York", "Los Angeles", "Chicago", "Dallas"]),
            # Data quality issues
            "supplier_id": maybe_null(f"SUP-{random.randint(100, 999)}", 0.1),
        }


# =============================================================================
# EVENT GENERATORS
# =============================================================================

class ClickstreamGenerator:
    """Generates realistic clickstream events."""
    
    def __init__(self, session_manager: UserSessionManager, inventory_manager: InventoryManager):
        self.session_manager = session_manager
        self.inventory_manager = inventory_manager
        self.event_sequence = 0
    
    def generate_event(self) -> Dict:
        """Generate a single clickstream event."""
        session, user = self.session_manager.get_or_create_session()
        session_data = self.session_manager.active_sessions.get(session["session_id"], {})
        funnel_stage = session_data.get("funnel_stage", "browsing")
        
        # Determine event type based on funnel stage and probabilities
        event_type = self._determine_event_type(funnel_stage, session["session_id"])
        
        # Generate event details based on type
        event = self._create_event(event_type, session, user)
        
        # Update funnel stage
        self._update_funnel(session["session_id"], event_type)
        
        self.event_sequence += 1
        return event
    
    def _determine_event_type(self, funnel_stage: str, session_id: str) -> str:
        """Determine next event type based on funnel stage."""
        cart = self.session_manager.get_cart(session_id)
        
        if funnel_stage == "browsing":
            return random.choices(
                ["page_view", "search", "product_view", "category_view", "exit"],
                weights=[30, 15, 35, 15, 5]
            )[0]
        
        elif funnel_stage == "considering":
            return random.choices(
                ["product_view", "add_to_cart", "search", "page_view", "exit"],
                weights=[25, 30, 15, 20, 10]
            )[0]
        
        elif funnel_stage == "cart":
            if not cart:
                return "page_view"
            return random.choices(
                ["view_cart", "checkout_start", "add_to_cart", "remove_from_cart", "continue_shopping", "exit"],
                weights=[15, 25, 15, 10, 20, 15]
            )[0]
        
        elif funnel_stage == "checkout":
            return random.choices(
                ["checkout_shipping", "checkout_payment", "checkout_confirm", "purchase", "checkout_abandon", "exit"],
                weights=[20, 20, 15, 30, 10, 5]
            )[0]
        
        return "page_view"
    
    def _create_event(self, event_type: str, session: Dict, user: Dict) -> Dict:
        """Create event with all details."""
        timestamp, is_late = generate_late_timestamp(datetime.now())
        
        # Base event structure
        event = {
            "event_id": generate_uuid(),
            "event_type": event_type,
            "event_timestamp": timestamp.isoformat(),
            "event_date": timestamp.strftime("%Y-%m-%d"),
            "event_hour": timestamp.hour,
            "is_late_arriving": is_late,
            
            # User info
            "user_id": user["user_id"],
            "is_anonymous": user.get("is_anonymous", True),
            "loyalty_tier": user.get("loyalty_tier"),
            
            # Session info
            "session_id": session["session_id"],
            "device_type": session["device_type"],
            "device_os": session["device_os"],
            "browser": session["browser"],
            "browser_version": session["browser_version"],
            "screen_resolution": session["screen_resolution"],
            
            # Traffic info
            "traffic_source": session["traffic_source"],
            "traffic_medium": session["traffic_medium"],
            "campaign": session.get("campaign"),
            
            # Location info
            "country_code": session["country_code"],
            "city": maybe_null(session["city"], 0.05),
            "ip_address": maybe_corrupt(session["ip_address"], 0.02),
            
            # Page info (will be populated based on event type)
            "page_url": None,
            "page_title": None,
            "referrer_url": None,
            
            # Product info (if applicable)
            "product_id": None,
            "product_name": None,
            "product_category": None,
            "product_subcategory": None,
            "product_brand": None,
            "product_price": None,
            "quantity": None,
            
            # Search info (if applicable)
            "search_query": None,
            "search_results_count": None,
            
            # Cart info (if applicable)
            "cart_id": None,
            "cart_total": None,
            "cart_item_count": None,
            
            # Order info (if applicable)
            "order_id": None,
            
            # Engagement metrics
            "time_on_page_seconds": random.randint(5, 300) if event_type != "exit" else 0,
            "scroll_depth_percent": random.randint(0, 100) if event_type not in ["exit", "purchase"] else None,
            "click_count": random.randint(0, 10),
            
            # Sequence tracking
            "event_sequence": self.event_sequence,
            "is_entrance": self.event_sequence == 0,
            "is_exit": event_type == "exit",
            
            # Data quality fields (for testing)
            "_data_quality_score": round(random.uniform(0.7, 1.0), 2),
            "_processing_timestamp": None,  # Will be filled by Spark
        }
        
        # Populate event-specific fields
        self._populate_event_details(event, event_type, session)
        
        # Introduce some duplicates (realistic issue)
        if random.random() < 0.02:
            event["_is_duplicate"] = True
        
        return event
    
    def _populate_event_details(self, event: Dict, event_type: str, session: Dict):
        """Populate event-type specific fields."""
        
        if event_type == "page_view":
            page = random.choice(["/", "/about", "/contact", "/help", "/faq"])
            event["page_url"] = page
            event["page_title"] = page.replace("/", " ").strip().title() or "Home"
            
        elif event_type == "search":
            query = random.choice(SEARCH_QUERIES)
            event["search_query"] = query
            event["search_results_count"] = random.randint(0, 500) if query else 0
            event["page_url"] = f"/search?q={query}" if query else "/search"
            event["page_title"] = f"Search: {query}" if query else "Search"
            
        elif event_type in ["product_view", "add_to_cart", "remove_from_cart"]:
            product = random.choice(PRODUCTS)
            event["product_id"] = product["product_id"]
            event["product_name"] = product["name"]
            event["product_category"] = product["category"]
            event["product_subcategory"] = product["subcategory"]
            event["product_brand"] = product["brand"]
            event["product_price"] = product["base_price"]
            event["quantity"] = random.randint(1, 3) if event_type == "add_to_cart" else 1
            event["page_url"] = f"/product/{product['product_id']}"
            event["page_title"] = product["name"]
            
            if event_type == "add_to_cart":
                self.session_manager.add_to_cart(session["session_id"], product, event["quantity"])
                
        elif event_type == "category_view":
            category = random.choice(list(set(p["category"] for p in PRODUCTS)))
            event["product_category"] = category
            event["page_url"] = f"/category/{category.lower().replace(' ', '-')}"
            event["page_title"] = category
            
        elif event_type in ["view_cart", "checkout_start", "checkout_shipping", 
                            "checkout_payment", "checkout_confirm"]:
            cart = self.session_manager.get_cart(session["session_id"])
            event["cart_id"] = f"CART-{session['session_id'][:8]}"
            event["cart_total"] = sum(item["price"] * item["quantity"] for item in cart)
            event["cart_item_count"] = sum(item["quantity"] for item in cart)
            
            page_map = {
                "view_cart": "/cart",
                "checkout_start": "/checkout",
                "checkout_shipping": "/checkout/shipping",
                "checkout_payment": "/checkout/payment",
                "checkout_confirm": "/checkout/confirm",
            }
            event["page_url"] = page_map.get(event_type, "/checkout")
            event["page_title"] = event_type.replace("_", " ").title()
            
        elif event_type == "purchase":
            cart = self.session_manager.get_cart(session["session_id"])
            event["order_id"] = f"ORD-{generate_uuid()[:8].upper()}"
            event["cart_total"] = sum(item["price"] * item["quantity"] for item in cart)
            event["cart_item_count"] = sum(item["quantity"] for item in cart)
            event["page_url"] = f"/order/confirmation/{event['order_id']}"
            event["page_title"] = "Order Confirmation"
    
    def _update_funnel(self, session_id: str, event_type: str):
        """Update session funnel stage based on event."""
        stage_transitions = {
            "product_view": "considering",
            "add_to_cart": "cart",
            "view_cart": "cart",
            "checkout_start": "checkout",
            "checkout_shipping": "checkout",
            "checkout_payment": "checkout",
            "checkout_confirm": "checkout",
            "purchase": "purchased",
            "exit": "exited",
            "checkout_abandon": "abandoned",
        }
        
        new_stage = stage_transitions.get(event_type)
        if new_stage:
            self.session_manager.update_session_stage(session_id, new_stage)
            
            # Clean up completed sessions
            if new_stage in ["purchased", "exited"]:
                if new_stage == "purchased":
                    self.session_manager.clear_cart(session_id)
                # Don't immediately remove - keep for some follow-up events
                if random.random() < 0.5:
                    self.session_manager.end_session(session_id)


class OrderGenerator:
    """Generates realistic order events."""
    
    def __init__(self, session_manager: UserSessionManager, inventory_manager: InventoryManager):
        self.session_manager = session_manager
        self.inventory_manager = inventory_manager
        self.pending_orders: Dict[str, Dict] = {}
    
    def generate_new_order(self, session: Dict, user: Dict, cart: List[Dict]) -> Dict:
        """Generate a new order from cart contents."""
        if not cart:
            return None
        
        timestamp, is_late = generate_late_timestamp(datetime.now())
        order_id = f"ORD-{generate_uuid()[:12].upper()}"
        
        # Calculate totals
        subtotal = sum(item["price"] * item["quantity"] for item in cart)
        shipping = weighted_choice(SHIPPING_METHODS)
        shipping_cost = shipping["cost"] if subtotal < 100 else 0  # Free shipping over $100
        
        # Maybe apply coupon
        coupon = random.choice(COUPON_CODES)
        discount = 0
        if coupon and subtotal >= coupon.get("min_order", 0):
            if coupon["discount_type"] == "percentage":
                discount = subtotal * (coupon["discount_value"] / 100)
            elif coupon["discount_type"] == "fixed":
                discount = coupon["discount_value"]
            elif coupon["discount_type"] == "free_shipping":
                shipping_cost = 0
        
        tax_rate = 0.08  # 8% tax
        tax = (subtotal - discount) * tax_rate
        total = subtotal - discount + shipping_cost + tax
        
        payment = weighted_choice(PAYMENT_METHODS)
        
        order = {
            "event_id": generate_uuid(),
            "event_type": "order_created",
            "event_timestamp": timestamp.isoformat(),
            "is_late_arriving": is_late,
            
            # Order info
            "order_id": order_id,
            "order_status": "pending",
            "order_date": timestamp.strftime("%Y-%m-%d"),
            "order_time": timestamp.strftime("%H:%M:%S"),
            
            # Customer info
            "user_id": user["user_id"],
            "customer_email": maybe_null(user.get("email"), 0.05),
            "customer_first_name": user.get("first_name"),
            "customer_last_name": user.get("last_name"),
            "customer_phone": user.get("phone"),
            "is_guest_checkout": user.get("is_anonymous", False),
            "loyalty_tier": user.get("loyalty_tier"),
            
            # Session info
            "session_id": session["session_id"],
            "device_type": session["device_type"],
            "traffic_source": session["traffic_source"],
            
            # Address info
            "shipping_country": user["country_code"],
            "shipping_city": user.get("city"),
            "shipping_postal_code": user.get("postal_code"),
            "shipping_address_line1": fake.street_address(),
            "shipping_address_line2": fake.secondary_address() if random.random() > 0.7 else None,
            
            "billing_same_as_shipping": random.random() > 0.2,
            
            # Items
            "items": cart,
            "item_count": sum(item["quantity"] for item in cart),
            "unique_item_count": len(cart),
            
            # Financials
            "subtotal": round(subtotal, 2),
            "discount_amount": round(discount, 2),
            "coupon_code": coupon["code"] if coupon and discount > 0 else None,
            "shipping_cost": round(shipping_cost, 2),
            "shipping_method": shipping["method"],
            "estimated_delivery_days": shipping["days"],
            "tax_amount": round(tax, 2),
            "tax_rate": tax_rate,
            "total_amount": round(total, 2),
            "currency": "USD",
            
            # Payment
            "payment_method": payment["method"],
            "payment_provider": payment["provider"],
            "payment_status": "pending",
            
            # Timestamps
            "created_at": timestamp.isoformat(),
            "updated_at": timestamp.isoformat(),
            "shipped_at": None,
            "delivered_at": None,
            
            # Fulfillment
            "warehouse_id": random.choice(["WH-001", "WH-002", "WH-003"]),
            "tracking_number": None,
            "carrier": None,
            
            # Flags
            "is_first_order": random.random() > 0.7,
            "is_gift": random.random() > 0.9,
            "gift_message": fake.sentence() if random.random() > 0.95 else None,
            
            # Data quality
            "_data_quality_score": round(random.uniform(0.8, 1.0), 2),
        }
        
        self.pending_orders[order_id] = order
        return order
    
    def generate_order_update(self) -> Optional[Dict]:
        """Generate an order status update for a pending order."""
        if not self.pending_orders:
            return None
        
        order_id = random.choice(list(self.pending_orders.keys()))
        order = self.pending_orders[order_id]
        current_status = order["order_status"]
        
        # Status progression
        status_flow = {
            "pending": ["confirmed", "cancelled"],
            "confirmed": ["processing", "cancelled"],
            "processing": ["shipped", "cancelled"],
            "shipped": ["delivered"],
            "delivered": [],
            "cancelled": ["refunded"],
            "refunded": [],
        }
        
        next_statuses = status_flow.get(current_status, [])
        if not next_statuses:
            del self.pending_orders[order_id]
            return None
        
        # Weight towards progression (80% progress, 20% cancel)
        if "cancelled" in next_statuses and len(next_statuses) > 1:
            weights = [80 / (len(next_statuses) - 1)] * (len(next_statuses) - 1) + [20]
            new_status = random.choices(next_statuses, weights=weights)[0]
        else:
            new_status = random.choice(next_statuses)
        
        timestamp = datetime.now()
        
        update_event = {
            "event_id": generate_uuid(),
            "event_type": "order_updated",
            "event_timestamp": timestamp.isoformat(),
            "is_late_arriving": False,
            
            "order_id": order_id,
            "user_id": order["user_id"],
            "previous_status": current_status,
            "new_status": new_status,
            "status_reason": self._get_status_reason(new_status),
            
            "updated_at": timestamp.isoformat(),
            "updated_by": "system",
        }
        
        # Add status-specific fields
        if new_status == "shipped":
            update_event["tracking_number"] = f"TRK{random.randint(100000000, 999999999)}"
            update_event["carrier"] = random.choice(["UPS", "FedEx", "USPS", "DHL"])
            update_event["shipped_at"] = timestamp.isoformat()
            
        elif new_status == "delivered":
            update_event["delivered_at"] = timestamp.isoformat()
            update_event["delivery_signature"] = fake.name() if random.random() > 0.5 else None
            
        elif new_status == "cancelled":
            update_event["cancellation_reason"] = random.choice([
                "customer_request", "payment_failed", "out_of_stock", 
                "fraud_suspected", "address_issue"
            ])
            
        elif new_status == "refunded":
            update_event["refund_amount"] = order["total_amount"]
            update_event["refund_method"] = "original_payment"
        
        # Update pending order
        order["order_status"] = new_status
        order["updated_at"] = timestamp.isoformat()
        
        if new_status in ["delivered", "refunded"]:
            del self.pending_orders[order_id]
        
        return update_event
    
    def _get_status_reason(self, status: str) -> str:
        """Get reason for status change."""
        reasons = {
            "confirmed": "Payment verified",
            "processing": "Order picked and packed",
            "shipped": "Handed to carrier",
            "delivered": "Customer received",
            "cancelled": "Order cancelled",
            "refunded": "Refund processed",
        }
        return reasons.get(status, "Status updated")


# =============================================================================
# KAFKA PRODUCER
# =============================================================================

class EcommerceEventProducer:
    """Main producer that orchestrates all event generation."""
    
    def __init__(self, bootstrap_servers: List[str] = None):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.producer = None
        
        self.session_manager = UserSessionManager()
        self.inventory_manager = InventoryManager()
        self.clickstream_generator = ClickstreamGenerator(self.session_manager, self.inventory_manager)
        self.order_generator = OrderGenerator(self.session_manager, self.inventory_manager)
        
        self.stats = {
            "clickstream": 0,
            "orders": 0,
            "inventory": 0,
            "errors": 0,
        }
    
    def connect(self):
        """Connect to Kafka."""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=5,
            retry_backoff_ms=1000,
            acks='all',
        )
        print(f"Connected to Kafka: {self.bootstrap_servers}")
    
    def send_event(self, topic: str, event: Dict, key: str = None):
        """Send event to Kafka topic."""
        try:
            future = self.producer.send(topic, value=event, key=key)
            future.get(timeout=10)  # Wait for confirmation
            return True
        except Exception as e:
            print(f"Error sending to {topic}: {e}")
            self.stats["errors"] += 1
            return False
    
    def generate_clickstream_event(self) -> Dict:
        """Generate and send clickstream event."""
        event = self.clickstream_generator.generate_event()
        
        if self.send_event(CLICKSTREAM_TOPIC, event, key=event["session_id"]):
            self.stats["clickstream"] += 1
            
            # If purchase event, also generate order
            if event["event_type"] == "purchase" and event.get("order_id"):
                self._generate_order_from_purchase(event)
        
        return event
    
    def _generate_order_from_purchase(self, purchase_event: Dict):
        """Generate order event from purchase clickstream event."""
        session_id = purchase_event["session_id"]
        session_data = self.session_manager.active_sessions.get(session_id, {})
        
        if not session_data:
            return
        
        cart = self.session_manager.get_cart(session_id)
        if not cart:
            return
        
        order = self.order_generator.generate_new_order(
            session_data["session"],
            session_data["user"],
            cart
        )
        
        if order:
            if self.send_event(ORDERS_TOPIC, order, key=order["order_id"]):
                self.stats["orders"] += 1
                
                # Generate inventory events for sold items
                for item in cart:
                    inv_event = self.inventory_manager.generate_inventory_event(
                        "sale",
                        item["product_id"],
                        -item["quantity"],
                        reason=f"Order {order['order_id']}"
                    )
                    self.send_event(INVENTORY_TOPIC, inv_event, key=item["product_id"])
                    self.inventory_manager.confirm_sale(item["product_id"], item["quantity"])
                    self.stats["inventory"] += 1
    
    def generate_order_update(self):
        """Generate order status update."""
        update = self.order_generator.generate_order_update()
        if update:
            if self.send_event(ORDERS_TOPIC, update, key=update["order_id"]):
                self.stats["orders"] += 1
    
    def generate_direct_order(self):
        """Generate a direct order event (independent of clickstream funnel)."""
        # Create a user and session for this order
        is_registered = random.random() > 0.3  # 70% registered users for orders
        user = self.session_manager.create_user(is_registered)
        session = self.session_manager.create_session(user)

        # Build a random cart
        num_items = random.randint(1, 5)
        cart = []
        for _ in range(num_items):
            product = random.choice(PRODUCTS)
            quantity = random.randint(1, 3)
            cart.append({
                "product_id": product["product_id"],
                "product_name": product["name"],
                "price": product["base_price"],
                "quantity": quantity,
            })

        # Generate the order
        order = self.order_generator.generate_new_order(session, user, cart)

        if order:
            if self.send_event(ORDERS_TOPIC, order, key=order["order_id"]):
                self.stats["orders"] += 1

                # Generate inventory events for sold items
                for item in cart:
                    inv_event = self.inventory_manager.generate_inventory_event(
                        "sale",
                        item["product_id"],
                        -item["quantity"],
                        reason=f"Order {order['order_id']}"
                    )
                    self.send_event(INVENTORY_TOPIC, inv_event, key=item["product_id"])
                    self.inventory_manager.confirm_sale(item["product_id"], item["quantity"])
                    self.stats["inventory"] += 1

        # Cleanup the temporary session
        self.session_manager.end_session(session["session_id"])

        return order

    def generate_inventory_event(self):
        """Generate random inventory event (restock, adjustment)."""
        product = random.choice(PRODUCTS)
        
        event_types = [
            ("restock", random.randint(50, 200), "Scheduled restock"),
            ("adjustment", random.randint(-5, 5), "Inventory audit"),
            ("damaged", -random.randint(1, 3), "Damaged goods"),
        ]
        
        event_type, qty_change, reason = random.choice(event_types)
        
        if event_type == "restock":
            self.inventory_manager.restock(product["product_id"], qty_change)
        
        event = self.inventory_manager.generate_inventory_event(
            event_type, product["product_id"], qty_change, reason
        )
        
        if self.send_event(INVENTORY_TOPIC, event, key=product["product_id"]):
            self.stats["inventory"] += 1
    
    def run(self, events_per_second: float = 10, duration_seconds: int = None):
        """
        Run the event generator.
        
        Args:
            events_per_second: Target rate of events
            duration_seconds: How long to run (None = forever)
        """
        self.connect()
        
        interval = 1.0 / events_per_second
        start_time = time.time()
        event_count = 0
        
        print(f"\nStarting event generation at {events_per_second} events/sec")
        print(f"Topics: {CLICKSTREAM_TOPIC}, {ORDERS_TOPIC}, {INVENTORY_TOPIC}")
        print("-" * 60)
        
        try:
            while True:
                # Check duration
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                
                # Generate events with weighted distribution
                event_type = random.choices(
                    ["clickstream", "order_new", "order_update", "inventory"],
                    weights=[70, 10, 10, 10]  # 70% clickstream, 10% new orders, 10% order updates, 10% inventory
                )[0]

                if event_type == "clickstream":
                    event = self.generate_clickstream_event()
                    print(f"[CLICK] {event['event_type']:20} | User: {event['user_id'][:8]}... | Session: {event['session_id'][:8]}...")

                elif event_type == "order_new":
                    order = self.generate_direct_order()
                    if order:
                        print(f"[ORDER] new_order              | Order: {order['order_id']} | Total: ${order['total_amount']:.2f}")

                elif event_type == "order_update":
                    self.generate_order_update()

                elif event_type == "inventory":
                    self.generate_inventory_event()
                
                event_count += 1
                
                # Print stats every 100 events
                if event_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    print(f"\n--- Stats: {event_count} events | {rate:.1f}/sec | "
                          f"Click: {self.stats['clickstream']} | Orders: {self.stats['orders']} | "
                          f"Inventory: {self.stats['inventory']} | Errors: {self.stats['errors']} ---\n")
                
                # Cleanup old sessions periodically
                if event_count % 50 == 0:
                    self.session_manager.cleanup_old_sessions()
                
                # Sleep to maintain rate
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\nShutting down...")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            
            print("\n" + "=" * 60)
            print("FINAL STATISTICS")
            print("=" * 60)
            print(f"Total Events Generated: {event_count}")
            print(f"  - Clickstream: {self.stats['clickstream']}")
            print(f"  - Orders: {self.stats['orders']}")
            print(f"  - Inventory: {self.stats['inventory']}")
            print(f"  - Errors: {self.stats['errors']}")
            print(f"Duration: {time.time() - start_time:.1f} seconds")
            print("=" * 60)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="E-Commerce Event Generator")
    parser.add_argument("--rate", type=float, default=1, help="Events per second")
    parser.add_argument("--duration", type=int, default=None, help="Duration in seconds (default: run forever)")
    parser.add_argument("--kafka", type=str, default="kafka:9093", help="Kafka bootstrap servers")
    
    args = parser.parse_args()
    
    global KAFKA_BOOTSTRAP_SERVERS
    KAFKA_BOOTSTRAP_SERVERS = [args.kafka]
    
    producer = EcommerceEventProducer()
    producer.run(events_per_second=args.rate, duration_seconds=args.duration)


if __name__ == "__main__":
    main()