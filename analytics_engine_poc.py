import datetime

# ==========================================
# 1. DATA MODELS
# ==========================================
class CampaignMetrics:
    """Stores performance data for a single digital marketing campaign."""
    def __init__(self, campaign_id, name):
        self.campaign_id = campaign_id
        self.name = name
        self.clicks = 0
        self.impressions = 0
        self.spend = 0.0

    def __str__(self):
        return f"[{self.campaign_id}] {self.name} | Clicks: {self.clicks}, Impressions: {self.impressions}"

# ==========================================
# 2. TRIE (PREFIX TREE) - For O(m) Searching
# ==========================================
class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.campaign_ids = [] # Stores IDs of campaigns that match this word

class Trie:
    """Prefix tree for rapid campaign name autocomplete and searching."""
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word, campaign_id):
        """Inserts a campaign name and its ID into the Trie."""
        current = self.root
        for char in word.lower():
            if char not in current.children:
                current.children[char] = TrieNode()
            current = current.children[char]
        current.is_end_of_word = True
        if campaign_id not in current.campaign_ids:
            current.campaign_ids.append(campaign_id)

    def search_prefix(self, prefix):
        """Returns a list of campaign IDs that match the given prefix."""
        current = self.root
        for char in prefix.lower():
            if char not in current.children:
                return [] # Prefix not found
            current = current.children[char]
        
        # If prefix exists, perform Depth-First Search (DFS) to gather all IDs
        return self._dfs_gather_ids(current)

    def _dfs_gather_ids(self, node):
        """Helper function to recursively gather all campaign IDs from a node down."""
        results = []
        if node.is_end_of_word:
            results.extend(node.campaign_ids)
        for child_node in node.children.values():
            results.extend(self._dfs_gather_ids(child_node))
        return list(set(results)) # Return unique IDs

# ==========================================
# 3. AVL TREE - For O(log n) Chronological Sorting
# ==========================================
class AVLNode:
    def __init__(self, timestamp, campaign_id, metrics_snapshot):
        self.timestamp = timestamp # The sorting key (e.g., "2026-03-01")
        self.campaign_id = campaign_id
        self.metrics_snapshot = metrics_snapshot
        self.left = None
        self.right = None
        self.height = 1

class AVLTree:
    """Self-balancing binary search tree for historical date-range queries."""
    
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
        """Inserts a historical record and automatically balances the tree."""
        # 1. Standard BST Insert
        if not root:
            return AVLNode(timestamp, campaign_id, metrics_snapshot)
        elif timestamp < root.timestamp:
            root.left = self.insert(root.left, timestamp, campaign_id, metrics_snapshot)
        else:
            root.right = self.insert(root.right, timestamp, campaign_id, metrics_snapshot)

        # 2. Update height
        root.height = 1 + max(self.get_height(root.left), self.get_height(root.right))

        # 3. Get balance factor to check if it became unbalanced
        balance = self.get_balance(root)

        # 4. Perform rotations if needed
        # Left Left Case
        if balance > 1 and timestamp < root.left.timestamp:
            return self.right_rotate(root)
        # Right Right Case
        if balance < -1 and timestamp > root.right.timestamp:
            return self.left_rotate(root)
        # Left Right Case
        if balance > 1 and timestamp > root.left.timestamp:
            root.left = self.left_rotate(root.left)
            return self.right_rotate(root)
        # Right Left Case
        if balance < -1 and timestamp < root.right.timestamp:
            root.right = self.right_rotate(root.right)
            return self.left_rotate(root)

        return root

    def get_date_range(self, root, start_date, end_date, results):
        """In-order traversal to retrieve records strictly within a date range."""
        if not root:
            return
        
        # If current node's date is greater than start_date, search left
        if root.timestamp > start_date:
            self.get_date_range(root.left, start_date, end_date, results)
        
        # If in range, append to results
        if start_date <= root.timestamp <= end_date:
            results.append({
                "date": root.timestamp,
                "campaign_id": root.campaign_id,
                "data": root.metrics_snapshot
            })
            
        # If current node's date is less than end_date, search right
        if root.timestamp < end_date:
            self.get_date_range(root.right, start_date, end_date, results)

# ==========================================
# 4. ORCHESTRATOR ENGINE (HASH MAP BASE)
# ==========================================
class AnalyticsEngine:
    """Main system integrating the Hash Map, Trie, and AVL Tree."""
    def __init__(self):
        self.campaign_registry = {} # Hash Map for O(1) lookups
        self.search_index = Trie()  # Trie for O(m) prefix searches
        self.timeline = AVLTree()   # AVL Tree for O(log n) date queries
        self.timeline_root = None

    def register_campaign(self, campaign_id, name):
        """Registers a campaign in both the Hash Map and the Trie."""
        try:
            if campaign_id in self.campaign_registry:
                raise ValueError("Campaign ID already exists.")
            
            # Insert into Hash Map
            self.campaign_registry[campaign_id] = CampaignMetrics(campaign_id, name)
            # Insert into Trie for searching
            self.search_index.insert(name, campaign_id)
            print(f"Success: Registered '{name}' (ID: {campaign_id})")
        except Exception as e:
            print(f"Error registering campaign: {e}")

    def log_event(self, campaign_id, event_type, value=1):
        """Updates real-time metrics in O(1) time."""
        try:
            if campaign_id not in self.campaign_registry:
                raise KeyError("Campaign ID not found.")
            
            if event_type == 'click':
                self.campaign_registry[campaign_id].clicks += value
            elif event_type == 'impression':
                self.campaign_registry[campaign_id].impressions += value
            else:
                raise ValueError("Invalid event type. Use 'click' or 'impression'.")
        except Exception as e:
            print(f"Error logging event: {e}")

    def save_daily_snapshot(self, date_str, campaign_id):
        """Saves a snapshot of current metrics into the AVL Tree."""
        if campaign_id in self.campaign_registry:
            metrics_copy = {
                "clicks": self.campaign_registry[campaign_id].clicks,
                "impressions": self.campaign_registry[campaign_id].impressions
            }
            self.timeline_root = self.timeline.insert(self.timeline_root, date_str, campaign_id, metrics_copy)

# ==========================================
# 5. DEMONSTRATION & TEST CASES
# ==========================================
def main():
    print("--- Initializing Digital Marketing Analytics Engine (PoC) ---")
    engine = AnalyticsEngine()

    print("\n[TEST 1] Registering Campaigns (Hash Map & Trie Insertion)")
    engine.register_campaign("C001", "Summer_Sale_Footwear")
    engine.register_campaign("C002", "Summer_Sale_Apparel")
    engine.register_campaign("C003", "Winter_Clearance")
    
    # Edge case testing
    engine.register_campaign("C001", "Duplicate_ID_Test") # Should trigger error handling

    print("\n[TEST 2] Logging Real-Time Events (O(1) Hash Map Updates)")
    engine.log_event("C001", "impression", 500)
    engine.log_event("C001", "click", 45)
    engine.log_event("C002", "impression", 1000)
    
    # Edge case testing
    engine.log_event("C999", "click", 1) # Should trigger error handling
    
    print("\nCurrent State of C001:", engine.campaign_registry["C001"])

    print("\n[TEST 3] Prefix Searching (O(m) Trie Traversal)")
    search_term = "Summer"
    print(f"Searching for prefix '{search_term}'...")
    matching_ids = engine.search_index.search_prefix(search_term)
    for cid in matching_ids:
        print(f" - Found match: {engine.campaign_registry[cid].name}")

    print("\n[TEST 4] Historical Data Sorting (O(log n) AVL Tree)")
    # Saving snapshots for chronological sorting
    engine.save_daily_snapshot("2026-03-01", "C001")
    engine.save_daily_snapshot("2026-03-05", "C001")
    engine.save_daily_snapshot("2026-03-10", "C002")
    engine.save_daily_snapshot("2026-03-15", "C003")

    start_date = "2026-03-02"
    end_date = "2026-03-12"
    print(f"Querying historical performance from {start_date} to {end_date}...")
    
    results = []
    engine.timeline.get_date_range(engine.timeline_root, start_date, end_date, results)
    for res in results:
        print(f" - Date: {res['date']} | Campaign: {res['campaign_id']} | Metrics: {res['data']}")

    print("\n--- PoC Demonstration Complete ---")

if __name__ == "__main__":
    main()