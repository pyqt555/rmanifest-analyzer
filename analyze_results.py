import numpy as np
import os

# File configurations
ACID_FILE = 'chunks.acid'
ACCESS_FILE = 'manifests.access'

# 32-byte layout: [ChunkID u64][FileHits u64][BigArrayOffset u64][LastManifestID u64]
acid_dtype = np.dtype([
    ('id', '<u8'),
    ('hits', '<u8'),
    ('offset', '<u8'),
    ('last', '<u8')
])

def main(n_orphans=20):
    if not os.path.exists(ACID_FILE) or not os.path.exists(ACCESS_FILE):
        print("Error: Required files missing.")
        return

    # 1. Memory map the files
    chunks = np.memmap(ACID_FILE, dtype=acid_dtype, mode='r')
    access = np.memmap(ACCESS_FILE, dtype='<u8', mode='r')

    # 2. Basic Statistics for File Hits
    hits = chunks['hits']
    print(f"--- File Access Statistics ({len(chunks):,} unique chunks) ---")
    print(f"Mean Accesses:   {np.mean(hits):.2f}")
    print(f"Median Accesses: {np.median(hits):.2f}")
    print(f"Std Deviation:   {np.std(hits):.2f}")
    print(f"Max Accesses:    {np.max(hits):,}")
    print(f"Min Accesses:    {np.min(hits):,}")
    
    # 3. Find Orphaned Chunks (File Hits == 0)
    # These exist in bundles but are not referenced in the manifest's file list
    orphan_indices = np.where(hits == 0)[0]
    num_orphans = len(orphan_indices)
    
    print(f"\nTotal Orphaned Chunks (0 File Hits): {num_orphans:,}")
    
    if num_orphans > 0:
        print("\n" + "="*85)
        print(f"{'ORPHANED CHUNK ID':<20} | {'MANIFESTS':<10} | {'SAMPLES OF CONTAINING MANIFESTS'}")
        print("-" * 85)
        
        # Determine manifest counts (offset[i+1] - offset[i])
        offsets = chunks['offset']
        counts = np.diff(offsets, append=len(access))

        for idx in orphan_indices[:n_orphans]:
            c_id = chunks['id'][idx]
            start = chunks['offset'][idx]
            count = counts[idx]
            
            # Pull manifest IDs from the big array
            manifest_ids = access[start : int(start + count)]
            manifest_hexes = [f"{m:016X}" for m in manifest_ids[:3]] # Show first 3
            
            m_str = ", ".join(manifest_hexes)
            if count > 3: m_str += f" (+{count-3} more)"
            
            print(f"{c_id:016X} | {count:<10} | {m_str}")
    else:
        print("\nNo chunks with 0 file hits found.")

if __name__ == "__main__":
    main()