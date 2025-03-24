def copy_first_n_lines(input_file_path, output_file_path, n=1000):
    """
    Copy the first n lines from a large input file to an output file.
    
    Args:
        input_file_path (str): Path to the large input file
        output_file_path (str): Path to the output file
        n (int): Number of lines to copy (default: 1000)
    """
    try:
        with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
            # Copy n lines
            for i, line in enumerate(input_file):
                if i >= n:
                    break
                output_file.write(line)
            
            # Report the number of lines copied
            print(f"Successfully copied {i+1} lines to {output_file_path}")
    except FileNotFoundError:
        print(f"Error: Input file '{input_file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) >= 3:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        num_lines = int(sys.argv[3]) if len(sys.argv) >= 4 else 1000
        copy_first_n_lines(input_file, output_file, num_lines)
    else:
        input_file = r"D:\USUARIOS\Osmany\Estudios\Master\05.CIS 5346.Storage Systems\FinalProject\cluster10.sort"
        output_file = r"D:\USUARIOS\Osmany\Estudios\Master\05.CIS 5346.Storage Systems\FinalProject\cluster10_sample.sort"
        num_lines = 1000
        copy_first_n_lines(input_file, output_file, num_lines)
        # print("Usage: python script.py <input_file> <output_file> [num_lines]")
        # print("Example: python script.py large_trace.txt sample_1000.txt 1000")