import time
import random
import string

# ==========================================
# 1. DATA MODELS
# ==========================================
class CampaignMetrics:
    def __init__(self, campaign_id, name):
        self.campaign_id = campaign_id
        self.name = name
        self.clicks = 0
        self.impressions = 0

# ==========================================
# 2. OPTIMIZED TRIE (PREFIX TREE)
# ==========================================
class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.campaign_ids = []

class Trie:
    def __init__(self):
        self.root = TrieNode()
        self.search_cache = {} # PHASE 3 OPTIMIZATION: Memoization Cache

    def insert(self, word, campaign_id):
        current = self.root
        for char in word.lower():
            if char not in current.children:
                current.children[char] = TrieNode()
            current = current.children[char]
        current.is_end_of_word = True
        if campaign_id not in current.campaign_ids:
            current.campaign_ids.append(campaign_id)
        
        # Clear cache on new insertion to prevent stale data
        self.search_cache.clear() 

    def search_prefix(self, prefix):
        prefix_lower = prefix.lower()
        
        # Check Cache first (O(1) retrieval)
        if prefix_lower in self.search_cache:
            return self.search_cache[prefix_lower], True # True indicates cache hit

        current = self.root
        for char in prefix_lower:
            if char not in current.children:
                return [], False
            current = current.children[char]
        
        # Perform DFS
        results = self._dfs_gather_ids(current)
        
        # Store in cache
        self.search_cache[prefix_lower] = results
        return results, False # False indicates cache miss

    def _dfs_gather_ids(self, node):
        results = []
        if node.is_end_of_word:
            results.extend(node.campaign_ids)
        for child_node in node.children.values():
            results.extend(self._dfs_gather_ids(child_node))
        return list(set(results))

# ==========================================
# 3. AVL TREE
# ==========================================
class AVLNode:
    def __init__(self, timestamp, campaign_id, metrics_snapshot):
        self.timestamp = timestamp
        self.campaign_id = campaign_id
        self.metrics_snapshot = metrics_snapshot
        self.left = None
        self.right = None
        self.height = 1

class AVLTree:
    def get_height(self, node):
        if not node: return 0
        return node.height

    def get_balance(self, node):
        if not node: return 0
        return self.get_height(node.left) - self.get_height(node.right)

    def right_rotate(self, y):
        x = y.left
        T2 = x.right
        x.right = y
        y.left = T2
        y.height = 1 + max(self.get_height(y.left), self.get_height(y.right))
        x.height = 1 + max(self.get_height(x.left), self.get_height(x.right))
        return x

    def left_rotate(self, x):
        y = x.right
        T2 = y.left
        y.left = x
        x.right = T2
        x.height = 1 + max(self.get_height(x.left), self.get_height(x.right))
        y.height = 1 + max(self.get_height(y.left), self.get_height(y.right))
        return y

    def insert(self, root, timestamp, campaign_id, metrics_snapshot):
        if not root:
            return AVLNode(timestamp, campaign_id, metrics_snapshot)
        elif timestamp < root.timestamp:
            root.left = self.insert(root.left, timestamp, campaign_id, metrics_snapshot)
        else:
            # Duplicates (timestamp == root.timestamp) go safely to the right
            root.right = self.insert(root.right, timestamp, campaign_id, metrics_snapshot)

        root.height = 1 + max(self.get_height(root.left), self.get_height(root.right))
        balance = self.get_balance(root)

        # Perform rotations using Child Balance Factors (Bulletproof against duplicates)
        # Left Left Case
        if balance > 1 and self.get_balance(root.left) >= 0:
            return self.right_rotate(root)
        # Right Right Case
        if balance < -1 and self.get_balance(root.right) <= 0:
            return self.left_rotate(root)
        # Left Right Case
        if balance > 1 and self.get_balance(root.left) < 0:
            root.left = self.left_rotate(root.left)
            return self.right_rotate(root)
        # Right Left Case
        if balance < -1 and self.get_balance(root.right) > 0:
            root.right = self.right_rotate(root.right)
            return self.left_rotate(root)

        return root

    def get_date_range(self, root, start_date, end_date, results):
        if not root: return
        if root.timestamp > start_date:
            self.get_date_range(root.left, start_date, end_date, results)
        if start_date <= root.timestamp <= end_date:
            results.append({"date": root.timestamp, "id": root.campaign_id})
        if root.timestamp < end_date:
            self.get_date_range(root.right, start_date, end_date, results)

# ==========================================
# 4. ORCHESTRATOR ENGINE
# ==========================================
class AnalyticsEngine:
    def __init__(self):
        self.campaign_registry = {} 
        self.search_index = Trie()  
        self.timeline = AVLTree()   
        self.timeline_root = None

    def register_campaign(self, campaign_id, name):
        if campaign_id not in self.campaign_registry:
            self.campaign_registry[campaign_id] = CampaignMetrics(campaign_id, name)
            self.search_index.insert(name, campaign_id)

    def log_event(self, campaign_id, event_type, value=1):
        if campaign_id in self.campaign_registry:
            if event_type == 'click':
                self.campaign_registry[campaign_id].clicks += value
            elif event_type == 'impression':
                self.campaign_registry[campaign_id].impressions += value

    def save_daily_snapshot(self, date_str, campaign_id):
        if campaign_id in self.campaign_registry:
            metrics_copy = {
                "clicks": self.campaign_registry[campaign_id].clicks,
                "impressions": self.campaign_registry[campaign_id].impressions
            }
            self.timeline_root = self.timeline.insert(self.timeline_root, date_str, campaign_id, metrics_copy)

# ==========================================
# 5. PHASE 3: STRESS TESTING & PROFILING
# ==========================================
def generate_random_string(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def run_stress_test():
    print("=== PHASE 3: SCALING AND OPTIMIZATION STRESS TEST ===")
    engine = AnalyticsEngine()
    
    # 1. SCALING: Massive Dataset Generation
    num_campaigns = 50000
    num_events = 500000
    print(f"\n[1] Scaling Test: Registering {num_campaigns} campaigns...")
    
    start_time = time.time()
    campaign_ids = []
    # Seed with a known prefix for testing
    engine.register_campaign("C_TEST", "Summer_Promo_2026")
    campaign_ids.append("C_TEST")
    
    for i in range(num_campaigns):
        c_id = f"C_{i}"
        c_name = generate_random_string(12)
        engine.register_campaign(c_id, c_name)
        campaign_ids.append(c_id)
        
    reg_time = time.time() - start_time
    print(f" -> Completed in {reg_time:.4f} seconds.")

    # 2. PERFORMANCE: Real-Time Ingestion (O(1) check)
    print(f"\n[2] Performance Test: Logging {num_events} random events...")
    start_time = time.time()
    for _ in range(num_events):
        random_cid = random.choice(campaign_ids)
        engine.log_event(random_cid, "click", 1)
    event_time = time.time() - start_time
    print(f" -> Completed in {event_time:.4f} seconds.")
    print(f" -> Average time per event: {(event_time/num_events):.8f} seconds (Proves O(1) efficiency)")

    # 3. OPTIMIZATION: Trie Caching / Memoization
    print("\n[3] Optimization Test: Trie Prefix Search ('Summer')")
    # First search (Cache Miss - Traverses Tree)
    start_time = time.time()
    results1, cached1 = engine.search_index.search_prefix("Summer")
    search_time_1 = time.time() - start_time
    print(f" -> Uncached Search Time: {search_time_1:.6f} seconds (Found {len(results1)} match)")

    # Second search (Cache Hit - O(1) lookup)
    start_time = time.time()
    results2, cached2 = engine.search_index.search_prefix("Summer")
    search_time_2 = time.time() - start_time
    print(f" -> Cached Search Time:   {search_time_2:.6f} seconds (Found {len(results2)} match)")
    if search_time_1 > 0:
        print(f" -> Optimization Gain: {((search_time_1 - search_time_2) / search_time_1) * 100:.2f}% faster")

    # 4. ADVANCED TESTING: AVL Tree Range Queries at Scale
    print("\n[4] Advanced AVL Testing: Inserting 10,000 chronological records...")
    start_time = time.time()
    for i in range(10000):
        # Generate random dates spanning roughly 10 years
        year = random.randint(2015, 2025)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        date_str = f"{year}-{month:02d}-{day:02d}"
        engine.save_daily_snapshot(date_str, "C_TEST")
    avl_insert_time = time.time() - start_time
    print(f" -> Insertion & Auto-Balancing completed in {avl_insert_time:.4f} seconds.")

    print("\nQuerying narrow range (2020-05-01 to 2020-05-30) out of 10,000 records...")
    results = []
    start_time = time.time()
    engine.timeline.get_date_range(engine.timeline_root, "2020-05-01", "2020-05-30", results)
    avl_search_time = time.time() - start_time
    print(f" -> Search completed in {avl_search_time:.6f} seconds. Retrieved {len(results)} records.")

if __name__ == "__main__":
    run_stress_test()