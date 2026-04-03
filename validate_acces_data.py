import numpy as np
import os
import sys

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

def validate():
    if not os.path.exists(ACID_FILE) or not os.path.exists(ACCESS_FILE):
        print("Error: Required files (.acid or .access) are missing.")
        return

    # 1. Memory map the files
    chunks = np.memmap(ACID_FILE, dtype=acid_dtype, mode='r')
    access = np.memmap(ACCESS_FILE, dtype='<u8', mode='r')

    num_chunks = len(chunks)
    num_access_entries = len(access)

    print(f"Loaded {num_chunks:,} chunks and {num_access_entries:,} access entries.")
    print("Starting validation...")

    # 2. Calculate manifest counts for each chunk
    # Since offset[i] is the start, the count for chunk i is offset[i+1] - offset[i]
    offsets = chunks['offset']
    
    # We calculate the counts by looking at the distance between offsets
    # For the last chunk, we use the total size of the access file as the end boundary
    counts = np.diff(offsets, append=num_access_entries)

    errors = 0
    checked = 0
    
    # 3. Validation Loop
    # We use a sampling or a full loop. For 100 files, a full loop is fast.
    for i in range(num_chunks):
        chunk_id = chunks['id'][i]
        last_manifest = chunks['last'][i]
        start_idx = chunks['offset'][i]
        end_idx = int(start_idx + counts[i])

        # Extract the slice from the big array
    
        manifest_list = access[start_idx:end_idx]

        # VALIDATION RULE: The 'last' manifest ID must exist in the manifest_list
        if last_manifest not in manifest_list:
            print(f"\n[!] VALIDATION FAILED for Chunk {chunk_id:016X}")
            print(f"    Expected manifest {last_manifest:016X} to be in the list.")
            print(f"    List contains {len(manifest_list)} entries.")
            print(f"    Index range: {start_idx} to {end_idx}")
            errors += 1
            
            # Stop after 10 errors to avoid spam
            if errors >= 10:
                print("\nToo many errors, stopping...")
                break
        
        checked += 1
        if checked % 100000 == 0:
            sys.stdout.write(f"\rValidated {checked:,} / {num_chunks:,} chunks...")
            sys.stdout.flush()

    print(f"\rValidated {checked:,} / {num_chunks:,} chunks.      ")
    
    if errors == 0:
        print("\nSUCCESS: All 'last' manifest markers were found in the access file.")
    else:
        print(f"\nFAILURE: Found {errors} integrity errors.")

if __name__ == "__main__":
    validate()