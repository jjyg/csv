#ifndef PAGE_TREE_H
#define PAGE_TREE_H

#include <vector>

#include "mmap_alloc.h"

/*
 * Data structure designed to store a sparse array with low memory overhead, quick access, and limited page cache usage
 * The array indexes are uint64_t, and the structure may hold multiple values for a given index (ie it's a hash table)
 * 
 * This is a n-tree with n = sizeof(memory page) / sizeof(pointer)
 * Internal memory is allocated with a page granularity, using a mmap_alloc object
 * 
 * The tree associates an integral index with an arbitrary memory structure (all values in the tree having the same size)
 * 
 * The target architecture is amd64, which means sizeof(pointer) = 8 and sizeof(memory page) = 4096
 *
 * Each node of the tree stores up to 4096/8 = 512 entries
 *
 * The leaves hold all entries, sorted
 *
 * For the leaves, the data structure is: 
 *  - one page of array indexes (eg [1, 6, 7, 8, 8, 10])
 *  - the next page(s) holds the values associated to the indexes (eg [v[1], v[6], v[7]...])
 *  - this way, when looking for a value, all necessary data (the indexes) is packed in a single memory page.
 *
 * The upper nodes of the tree have the following structure:
 *  - each entry of the node points to one leaf (last level) or node (upper level)
 *  - the 1st page of the node holds the lowest index in the corresponding subtree
 *  - the 2nd&3rd page of the node holds the pointer to the subtree and the number of elements in the node or leaf
 *
 * The tree depth is variable, and adapts to the total number of elements in the tree.
 *
 * When inserting a value, insert it sorted inside the corresponding leaf (memmove values with higher indexes).
 * If the leaf is full, allocate a new leaf, and split the previous one in two. Update the associated nodes upper up the tree.
 * This way, all the tree nodes are at least half full (except the last node for each depth level).
 *
 * To handle duplicate indexes efficiently, all entries with one index value can only be stored in a single leaf (ie splitting a leaf so that both halfs hold
 *  values with the same index is forbidden), except if the leaf is already filled with entries having the same index. IE the only way to have leaf(n+1) starting
 *  with index i and having leaf(n) ending with index i is to have leaf(n) is full and also start with index i.
 * If we didn't do this, when looking for an index that is the first of a leaf we'd have to check the previous leaf too.
 */

class page_tree
{
public:
	typedef uint64_t t_idx;	// TODO template?

private:
	unsigned value_malloc_size;	/* size of the memory to allocate for one value in the leaves */

	mmap_alloc mm_nodes;	/* store nodes in RAM (multi-GB trees will only have a few MB of nodes) */
	mmap_alloc mm_leaves;	/* may use file-backed swap if desired ; holds indexes + values */
	const unsigned max_entry_per_node;	/* number of indexes stored per node - used to compute offset between indexes and values */

	struct t_node {
		void *ptr;
		unsigned count;
	};

	t_node tree_root;
	unsigned tree_depth;	/* number of nodes to traverse before we get to leaves */


	t_idx *node_to_idx( void *node, unsigned i = 0 )
	{
		return (t_idx *)node + i;
	}

	t_node *node_to_subnode( void *node, unsigned idx )
	{
		return (t_node *)node_to_idx( node, max_entry_per_node ) + idx;
	}

	void *node_to_value( void *node, unsigned idx )
	{
		return (void *)( (char *)node_to_idx( node, max_entry_per_node ) + idx * value_malloc_size );
	}

	void *alloc_node_page()
	{
		void *p = mm_nodes.alloc( ( sizeof(t_idx) + sizeof(t_node) ) * max_entry_per_node, sizeof(t_idx) );
		if ( !p )
			throw std::bad_alloc();
		return p;
	}

	void *alloc_leaf_page()
	{
		void *p = mm_leaves.alloc( ( sizeof(t_idx) + value_malloc_size ) * max_entry_per_node, sizeof(t_idx) );
		if ( !p )
			throw std::bad_alloc();
		return p;
	}

	/* given a hash, a pointer to a sorted hash array and the length of the array, return the index of first value <= a target value */
	int binsearch_index( t_idx value, t_idx *ary, unsigned count )
	{
		if ( !count )
			return -1;

		unsigned min = 0;
		unsigned max = count - 1;
		for (;;)
		{
			unsigned mid = (min + max) / 2;
			if ( ary[ mid ] > value )
			{
				if ( mid == 0 )
					return -1;

				if ( mid == min )
					return mid - 1;

				max = mid - 1;
			}
			else if ( ary[ mid ] == value )
			{
				while ( mid && ary[ mid - 1 ] == value )
					--mid;

				return mid;
			}
			else
			{
				if ( mid == max )
					return mid;

				min = mid + 1;
			}
		}
	}

	/* find_rec: create an iterator (list of index, per depth) for a hash value ; return the 1st associated value */
	void *find_rec( std::vector< unsigned > *iter, unsigned depth, t_idx idx, t_node *curnode )
	{
		/*
		 * we are called with a ptr to the t_node whose idx is the largest <= idx among its siblings
		 * so we know that idx is in this node but not in its following sibling (ie if cur.max_idx < idx, we can return NULL)
		 */

		t_idx *p_idx = node_to_idx( curnode->ptr );
		int i = binsearch_index( idx, p_idx, curnode->count );
		if ( i < 0 )
			return NULL;

		(*iter)[ depth ] = i;

		void *p = NULL;
		if ( depth == tree_depth )
		{
			if ( p_idx[ i ] == idx )
				p = node_to_value( curnode->ptr, i );
		}
		else
			p = find_rec( iter, depth + 1, idx, node_to_subnode( curnode->ptr, i ) );

		return p;
	}

	/* split a leaf in two
	 * populates new_leaf, updates old_leaf->count
	 * takes care not to split collision lists */
	void split_node( t_node *old_node, t_node *new_node, bool is_leaf )
	{
		unsigned split_idx = old_node->count / 2;
		t_idx *p_idx = node_to_idx( old_node->ptr );

		if ( p_idx[ split_idx ] == p_idx[ 0 ] )
		{
			if ( p_idx[ old_node->count - 1 ] != p_idx[ 0 ] )
			{
				/* collision sequence 0->split_idx, find the end */
				while ( p_idx[ split_idx ] == p_idx[ 0 ] )
					++split_idx;
			}
			/* else { collision page, split in the middle } */
		}
		else
		{
			/* split before sequence */
			while ( split_idx > 0 && p_idx[ split_idx ] == p_idx[ split_idx - 1 ] )
				--split_idx;
		}

		new_node->count = old_node->count - split_idx;
		old_node->count = split_idx;
		if ( is_leaf )
			new_node->ptr = alloc_leaf_page();
		else
			new_node->ptr = alloc_node_page();

		/* move indexes */
		memmove( new_node->ptr, (void *)node_to_idx( old_node->ptr, split_idx ), new_node->count * sizeof(t_idx) );

		/* move values */
		if ( is_leaf )
			memmove( node_to_value( new_node->ptr, 0 ), node_to_value( old_node->ptr, split_idx ), new_node->count * value_malloc_size );
		else
			memmove( node_to_subnode( new_node->ptr, 0 ), node_to_subnode( old_node->ptr, split_idx ), new_node->count * sizeof(t_node) );
	}

	/* insert_rec: inserts a new value at idx
	 * returns a pointer to the newly allocated value
	 * if a node is split, update curnode (low half) and populate sibling (upper half) */
	void *insert_rec( t_idx idx, t_node *curnode, t_node *sibling, unsigned depth )
	{
		void *value_ptr = NULL;
		t_idx new_idx = idx;
		bool is_leaf = ( depth == 0 );
		t_node splitted;

		if ( !is_leaf )
		{
			t_idx *p_idx = node_to_idx( curnode->ptr );
			int i = binsearch_index( idx, p_idx, curnode->count );
			if ( i < 0 )
				i = 0;

			t_node *subnode = node_to_subnode( curnode->ptr, i );
			splitted.ptr = NULL;
			splitted.count = 0;
			value_ptr = insert_rec( idx, subnode, &splitted, depth - 1 );

			/* update self.idx[ i ] */
			t_idx sub_idx = *node_to_idx( subnode->ptr );
			if ( sub_idx < p_idx[ i ] )
				p_idx[ i ] = sub_idx;

			if ( !splitted.ptr )
				return value_ptr;

			/* new subnode was allocated, insert it */
			new_idx = *node_to_idx( splitted.ptr );
		}

		/* insert new entry in node */
		t_node *node = curnode;
		if ( curnode->count >= max_entry_per_node )
		{
			split_node( curnode, sibling, is_leaf );

			/* check if we insert into curnode or sibling */
			if ( new_idx >= *node_to_idx( sibling->ptr ) )
				node = sibling;
		}

		t_idx *p_idx = node_to_idx( node->ptr );
		int i = binsearch_index( new_idx, p_idx, node->count );
		if ( i < 0 )
			i = 0;
		if ( (unsigned)i < node->count && p_idx[ i ] < new_idx )
			/* insert in last position */
			++i;

		if ( (unsigned)i < node->count )
		{
			/* move upper indexes to make room for idx */
			memmove( (void *)node_to_idx( node->ptr, i + 1 ), (void *)node_to_idx( node->ptr, i ), ( node->count - i ) * sizeof(t_idx) );
			if ( is_leaf )
				memmove( (void *)node_to_value( node->ptr, i + 1 ), (void *)node_to_value( node->ptr, i ), ( node->count - i ) * value_malloc_size );
			else
				memmove( (void *)node_to_subnode( node->ptr, i + 1 ), (void *)node_to_subnode( node->ptr, i ), ( node->count - i ) * sizeof(t_node) );
		}

		node->count++;
		node_to_idx( node->ptr )[ i ] = new_idx;

		if ( is_leaf )
			value_ptr = node_to_value( node->ptr, i );
		else
			*node_to_subnode( node->ptr, i ) = splitted;

		return value_ptr;
	}

public:
	explicit page_tree( const std::string &mmap_dir ) :
		value_malloc_size(0),
		mm_nodes(""),
		mm_leaves(mmap_dir),
		max_entry_per_node(4096 / sizeof(t_idx))
	{
		tree_root.ptr = NULL;
		tree_root.count = 0;
		tree_depth = 0;
	}

	/*
	 * sets the size in bytes of every value of the tree
	 * must be called before any insertion in the tree
	 * shall not be called after any insertion (resets the tree & leaks old memory)
	 */
	void set_value_size( unsigned sz )
	{
		value_malloc_size = sz;
		tree_root.ptr = alloc_leaf_page();
		tree_root.count = 0;
		tree_depth = 0;
	}

	/*
	 * insert a new entry, allocates the value
	 * returns the pointer to the value
	 */
	void *insert( t_idx idx )
	{
		t_node newnode = { NULL, 0 };
		void *value_ptr = insert_rec( idx, &tree_root, &newnode, tree_depth );

		if ( newnode.ptr )
		{
			/* increase tree depth */
			void *newroot = alloc_node_page();

			*node_to_idx( newroot, 0 ) = *node_to_idx( tree_root.ptr );
			*node_to_subnode( newroot, 0 ) = tree_root;
			*node_to_idx( newroot, 1 ) = *node_to_idx( newnode.ptr );
			*node_to_subnode( newroot, 1 ) = newnode;

			tree_root.ptr = newroot;
			tree_root.count = 2;

			++tree_depth;
		}

		return value_ptr;
	}

	/* release memory used by the iterator */
	void iter_free( void **raw_iter )
	{
		std::vector< unsigned > *iter = NULL;
		if ( raw_iter )
			iter = *(std::vector< unsigned > **)raw_iter;
		if ( iter )
			delete iter;
		if ( raw_iter )
			*raw_iter = NULL;
	}

	/* when called with a NULL iterator, return the first value of the tree and create iter
	 * when called with a non-NULL iterator issued from a previous call, returns the next value in the tree (ordered by index)
	 * returns NULL after the tree has been traversed (frees iter)
	 * do not insert new values during an iteration */
	void *iter_next( void **raw_iter )
	{
		std::vector< unsigned > *iter = NULL;
		if ( raw_iter )
			iter = *(std::vector< unsigned > **)raw_iter;

		if ( !iter )
		{
			iter = new std::vector< unsigned >( tree_depth + 1, 0 );
			if ( raw_iter )
				*raw_iter = (void *)iter;
		}
		else
		{
			if ( iter->size() != tree_depth + 1 )
				return NULL;

			(*iter)[ iter->size() - 1 ] += 1;
		}

		for (;;)
		{
			t_node *node = &tree_root;
			for ( unsigned i = 0 ; i <= tree_depth ; ++i )
			{
				if ( (*iter)[ i ] >= node->count )
				{
					if ( i == 0 )
					{
						delete iter;
						if ( raw_iter )
							*raw_iter = NULL;

						return NULL;
					}

					/* propagate carry, retry */
					(*iter)[ i - 1 ] += 1;
					for ( unsigned j = i ; j <= tree_depth ; ++j )
						(*iter)[ j ] = 0;

					break;
				}

				if ( i == tree_depth )
					return node_to_value( node->ptr, (*iter)[ i ] );
				else
					node = node_to_subnode( node->ptr, (*iter)[ i ] );
			}
		}
	}


	/* same as iter_next, but returns only entries for a given hash */
	void *find( t_idx idx, void **raw_iter = NULL )
	{
		std::vector< unsigned > *iter = NULL;
		if ( raw_iter )
			iter = *(std::vector< unsigned > **)raw_iter;

		if ( !iter )
		{
			/* return 1st match */
			iter = new std::vector< unsigned >( tree_depth + 1, 0 );
			void *out = find_rec( iter, 0, idx, &tree_root );
			if ( !out )
			{
				delete iter;
				return NULL;
			}

			if ( raw_iter )
				*raw_iter = (void *)iter;

			return out;
		}
		else
		{
			/* fetch next match, check idx */
			void *out = iter_next( raw_iter );
			if ( !out )
				return NULL;

			t_node *node = &tree_root;
			iter = *(std::vector< unsigned > **)raw_iter;
			for ( unsigned i = 0 ; i < tree_depth ; ++i )
				node = node_to_subnode( node->ptr, (*iter)[ i ] );

			if ( idx == *node_to_idx( node->ptr, (*iter)[ tree_depth ] ) )
				/* same index */
				return out;

			/* bad index, end iteration */
			delete iter;
			if ( raw_iter )
				*raw_iter = NULL;

			return NULL;
		}
	}

private:
	page_tree ( const page_tree& );
	page_tree& operator=( const page_tree& );
};

#endif
