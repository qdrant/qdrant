"""
Usage examples:
  # Basic usage - show aggregated cache percentages for all vector index files
  python smaps-view.py $(pidof qdrant) '.*/vector_index/.*'

  # Verbose mode - show individual files larger than 1MB
  python smaps-view.py $(pidof qdrant) '.*/vector_storage/.*' -v

Example output:
    Cache percentages for pattern '.vector_storage.*':
    r--s: 0.00%
    rw-s: 0.06%
"""

import re
import sys
import argparse
from typing import Dict, Tuple, List
from dataclasses import dataclass


# Example of smap format:
#
#
# 7de883800000-7de885800000 r--s 00000000 00:33 26610006                   /qdrant/storage/collections/benchmark/0/segments/6250c760-ccaf-468e-a96f-8e2b03f8c524/vector_storage/vectors/chunk_2.mmap
# Size:              32768 kB
# KernelPageSize:        4 kB
# MMUPageSize:           4 kB
# Rss:                   0 kB
# Pss:                   0 kB
# Pss_Dirty:             0 kB
# Shared_Clean:          0 kB
# Shared_Dirty:          0 kB
# Private_Clean:         0 kB
# Private_Dirty:         0 kB
# Referenced:            0 kB
# Anonymous:             0 kB
# KSM:                   0 kB
# LazyFree:              0 kB
# AnonHugePages:         0 kB
# ShmemPmdMapped:        0 kB
# FilePmdMapped:         0 kB
# Shared_Hugetlb:        0 kB
# Private_Hugetlb:       0 kB
# Swap:                  0 kB
# SwapPss:               0 kB
# Locked:                0 kB
# THPeligible:           0
# ProtectionKey:         0
# VmFlags: rd sh mr mw me ms sr sd 
#



@dataclass
class MapStats:
    size: int = 0
    rss: int = 0

def parse_smaps(smaps_content: str, pattern: str, verbose: bool = False) -> Dict[Tuple[str, str], MapStats]:
    """
    Parse smaps content and calculate size and RSS for files matching the pattern.
    Optionally logs individual file percentages for files larger than 1MB.
    
    Args:
        smaps_content: Content of /proc/{pid}/smaps file
        pattern: Regular expression pattern to match against file paths
        verbose: Whether to print per-file information
        
    Returns:
        Dictionary mapping (file_path, permissions) tuples to their stats
    """
    file_stats: Dict[Tuple[str, str], MapStats] = {}
    current_file = None
    current_size = 0
    current_rss = 0
    current_permissions = ""
    
    # Compile the regex pattern
    regex = re.compile(pattern)
    
    for line in smaps_content.splitlines():
        # Check if line starts with an address range (new memory map entry)
        if re.match(r'^[0-9a-f]+-[0-9a-f]+', line):
            # If we have a previous file that matched, add its stats
            if current_file and regex.search(current_file):
                key = (current_file, current_permissions)
                if key not in file_stats:
                    file_stats[key] = MapStats()
                
                stats = file_stats[key]
                stats.size += current_size
                stats.rss += current_rss
                
                # Log individual file percentage if verbose mode is enabled and file is larger than 1MB
                if verbose and current_size > 1024:
                    percentage = (current_rss / current_size) * 100
                    print(f"File: {current_file} ({current_permissions})")
                    print(f"  Size: {current_size} kB")
                    print(f"  RSS: {current_rss} kB")
                    print(f"  Cache percentage: {percentage:.2f}%")
                    print()
            
            # Extract file path and permissions from the line
            parts = line.split()
            current_file = parts[-1] if len(parts) > 5 else None
            current_size = 0
            current_rss = 0
            # Store full permissions string
            current_permissions = parts[1] if len(parts) > 1 else ""
        elif line.startswith('Size:'):
            current_size = int(line.split()[1])
        elif line.startswith('Rss:'):
            current_rss = int(line.split()[1])
    
    # Don't forget to add the last entry if it matches
    if current_file and regex.search(current_file):
        key = (current_file, current_permissions)
        if key not in file_stats:
            file_stats[key] = MapStats()
        
        stats = file_stats[key]
        stats.size += current_size
        stats.rss += current_rss
        
        # Log individual file percentage for the last entry if verbose mode is enabled and file is larger than 1MB
        if verbose and current_size > 1024:
            percentage = (current_rss / current_size) * 100 if current_size > 0 else 0
            print(f"File: {current_file} ({current_permissions})")
            print(f"  Size: {current_size} kB")
            print(f"  RSS: {current_rss} kB")
            print(f"  Cache percentage: {percentage:.2f}%")
            print()
    
    return file_stats

def calculate_cache_percentage(pid: int, pattern: str, verbose: bool = False) -> Dict[str, float]:
    """
    Calculate the percentage of memory that is cached for files matching the pattern.
    Separates by permission type.
    
    Args:
        pid: Process ID to analyze
        pattern: Regular expression pattern to match against file paths
        verbose: Whether to print per-file information
        
    Returns:
        Dictionary mapping permission strings to their cache percentages
    """
    try:
        with open(f'/proc/{pid}/smaps', 'r') as f:
            content = f.read()
        
        file_stats = parse_smaps(content, pattern, verbose)
        
        if not file_stats:
            return {}
            
        # Calculate percentages for each permission type
        permission_stats: Dict[str, Tuple[int, int]] = {}
        for (_, perms), stats in file_stats.items():
            if perms not in permission_stats:
                permission_stats[perms] = (0, 0)
            size, rss = permission_stats[perms]
            permission_stats[perms] = (size + stats.size, rss + stats.rss)
        
        # Calculate percentages
        percentages = {}
        for perms, (size, rss) in permission_stats.items():
            if size > 0:
                percentages[perms] = (rss / size) * 100
            else:
                percentages[perms] = 0.0
        
        return percentages
    except FileNotFoundError:
        print(f"Error: Could not find /proc/{pid}/smaps", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"Error processing smaps: {e}", file=sys.stderr)
        return {}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze memory maps and calculate cache percentages')
    parser.add_argument('pid', type=int, help='Process ID to analyze')
    parser.add_argument('pattern', type=str, help='Regular expression pattern to match against file paths')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print per-file information')
    
    args = parser.parse_args()
    
    percentages = calculate_cache_percentage(args.pid, args.pattern, args.verbose)
    print(f"Cache percentages for pattern '{args.pattern}':")
    for perms, percentage in percentages.items():
        print(f"  {perms}: {percentage:.2f}%")
