import re
import math
from typing import List, Dict, Union
from pandas import notna, DataFrame
from sentence_transformers import SentenceTransformer

class CategoryFinder:
    """
    A class to find relevant categories based on user queries.
    Uses semantic matching for more accurate results.
    """
    
    def __init__(self, batch_size: int = 1000):
        self.categories_df = None
        self.batch_size = batch_size
        self.processed = False
        
        # Initialize semantic model
        
        self.semantic_model = SentenceTransformer('all-mpnet-base-v2')
        self.semantic_vectors = []
    
    
    def load_data(self, data: DataFrame):
        """
        Load category data, either from a DataFrame or from a file path.
        
        Parameters:
        -----------
        data: 
            DataFrame containing category data or path to CSV file
        Returns:
        --------
        self
        """
        self.categories_df = data
            
        self.categories_df.columns = [col.strip() for col in self.categories_df.columns]
        
        # Create cleaned category names
        self.categories_df['category_name_clean'] = self.categories_df['category_name'].fillna('').apply(
            lambda x: re.sub(r'\s+', ' ', x.lower()).strip()
        )
        
        return self
    
    
    def process_data(self):
        """
        Process the category data to build the semantic embeddings.
        
        Returns:
        --------
        self
        """
        if self.categories_df is None:
            raise ValueError("Data must be loaded before processing. Use load_data() first.")
        
        # Process semantic embeddings
        texts_to_embed = self.categories_df['category_name_clean'].tolist()
        
        self.semantic_vectors = []
        for start_idx in range(0, len(texts_to_embed), self.batch_size):
            end_idx = min(start_idx + self.batch_size, len(texts_to_embed))
            batch_texts = texts_to_embed[start_idx:end_idx]
            batch_vectors = self.semantic_model.encode(batch_texts)
            self.semantic_vectors.extend(batch_vectors)
        
        self._create_lookup_tables()
        
        self.processed = True
        return self
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        
        # Calculate the magnitudes
        mag1 = math.sqrt(sum(a * a for a in vec1))
        mag2 = math.sqrt(sum(b * b for b in vec2))
        
        if mag1 == 0 or mag2 == 0:
            return 0
        
        # Return cosine similarity
        return dot_product / (mag1 * mag2)
        
    def _create_lookup_tables(self):
        self.category_name_lookup = {
            name.lower(): idx for idx, name in 
            enumerate(self.categories_df['category_name_clean'])
        }
        
        self.parent_child_map = {}
        for i in range(1, 6): 
            parent_id_col = f'level{i}_category_id'
            if parent_id_col in self.categories_df.columns:
                for _, row in self.categories_df.iterrows():
                    if notna(row.get(parent_id_col)):
                        parent_id = row[parent_id_col]
                        if parent_id not in self.parent_child_map:
                            self.parent_child_map[parent_id] = []
                        if notna(row.get('category_id')):
                            self.parent_child_map[parent_id].append(row['category_id'])
    
    def _find_categories(self, query: str, top_n: int = 5, threshold: float = 0.3, 
                        exact_match: bool = False) -> List[Dict]:
        if not self.processed:
            raise ValueError("Data must be processed before searching. Use process_data() first.")
        
        query = query.lower().strip()
        
        exact_matches = []
        normalized_query = query
        
        
        if normalized_query in self.category_name_lookup:
            idx = self.category_name_lookup[normalized_query]
            category = self.categories_df.iloc[idx]
            exact_matches.append({
                'category_id': category.get('category_id'),
                'category_name': category.get('category_name'),
                'category_label': category.get('category_label', ''),
                'similarity_score': 1.0,  # Perfect match
                'category_level': int(category.get('category_level', 0)),
                'match_type': 'exact'
            })
        
        
        if exact_matches and exact_match:
            return exact_matches
            
        
        results = exact_matches.copy()
        added_indices = {self.category_name_lookup.get(m['category_name'].lower(), -1) for m in exact_matches}
        
        # Create semantic vector for query
        query_semantic = self.semantic_model.encode([query])[0]
        
        
        semantic_similarities = [self._cosine_similarity(query_semantic, vec) for vec in self.semantic_vectors]
        
        similarity_pairs = [(idx, score) for idx, score in enumerate(semantic_similarities)]
        similarity_pairs.sort(key=lambda x: x[1], reverse=True)
        
        for idx, score in similarity_pairs:
            if len(results) >= top_n:
                break
                
            if score >= threshold and idx not in added_indices:
                added_indices.add(idx)
                category = self.categories_df.iloc[idx]
                results.append({
                    'category_id': category.get('category_id'),
                    'category_name': category.get('category_name'),
                    'category_label': category.get('category_label', ''),
                    'similarity_score': float(score),
                    'category_level': int(category.get('category_level', 0)),
                    'match_type': 'semantic'
                })
                
        return sorted(results, key=lambda x: x['similarity_score'], reverse=True)
    
    def suggest_categories(self, query: str, top_n, exact_match,verbose,as_df,hide_ids,list_return,show_name_and_id=False) -> Union[list[str], DataFrame]:
        """
        Suggest categories using exact matching and semantic matching.
        
        Parameters
        ----------
        query : str
            Category search query from user
        top_n : int
            Number of matches to return
        exact_match : bool, optional
            Whether to return exact matches only. Defaults to False
        verbose : bool, optional
            Whether to return matches with additional information. Defaults to False
        as_df : bool, optional
            Whether to return matches as a dataframe with additional information
            
        Returns
        -------
        Union[list[str], DataFrame]
            List or DataFrame of matches
        """
        
        matches = self._find_categories(query, top_n=top_n, exact_match=exact_match)
        
        # matches = self._remove_redundant_children(matches)
        df = DataFrame(matches)
        if hide_ids:
            df = df[df.columns.drop(list(df.filter(regex='category_id')))]
        
        if show_name_and_id:
            if verbose:
                print(df)
            else:
                print(df[["category_id","category_name"]])    
        if as_df:
            return df
        if list_return == "name":
            return [item['category_name'] for item in matches]
        else:
            return [item['category_id'] for item in matches]
    
    def _remove_redundant_children(self, categories: List[Dict]) -> List[Dict]:
        if not categories or len(categories) <= 1:
            return categories
            
        category_ids = {cat['category_id'] for cat in categories}
        
        children_to_remove = set()
        
        for category in categories:
            level = category.get('category_level', 0)
            category_id = category.get('category_id')
            
            for i in range(1, level):
                parent_level_col = f'level{i}_category_id'
                if parent_level_col in self.categories_df.columns:
                    cat_row = self.categories_df[self.categories_df['category_id'] == category_id]
                    if not cat_row.empty and parent_level_col in cat_row.columns:
                        parent_id = cat_row.iloc[0][parent_level_col]
                        # If the parent is in our results, mark this child for removal
                        if parent_id in category_ids:
                            children_to_remove.add(category_id)
                            break
        return [cat for cat in categories if cat['category_id'] not in children_to_remove]