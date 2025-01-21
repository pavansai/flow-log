import csv
from collections import defaultdict

def load_lookup_table(lookup_file):
    lookup_table = {}
    with open(lookup_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            key = (row['dstport'], row['protocol'].lower())
            lookup_table[key] = row['tag']
    return lookup_table

def parse_flow_logs(flow_log_file, lookup_table):
    tag_counts = defaultdict(int)
    port_protocol_counts = defaultdict(int)
    
    with open(flow_log_file, 'r') as file:
        for line in file:
            parts = line.split()
            dstport = parts[5]
            protocol = parts[6]
            key = (dstport, protocol.lower())
            tag = lookup_table.get(key, "Untagged")
            
            tag_counts[tag] += 1
            port_protocol_counts[key] += 1
    
    return tag_counts, port_protocol_counts

def write_output(tag_counts, port_protocol_counts, output_file):
    with open(output_file, 'w') as file:
        file.write("Tag Counts:\nTag,Count\n")
        for tag, count in tag_counts.items():
            file.write(f"{tag},{count}\n")
        
        file.write("\nPort/Protocol Combination Counts:\nPort,Protocol,Count\n")
        for (port, protocol), count in port_protocol_counts.items():
            file.write(f"{port},{protocol},{count}\n")

if __name__ == "__main__":
    lookup_file = 'lookup_table.csv'  # Replace with your lookup table file path
    flow_log_file = 'flow_logs.txt'   # Replace with your flow log file path
    output_file = 'output.txt'        # Output file path
    
    lookup_table = load_lookup_table(lookup_file)
    tag_counts, port_protocol_counts = parse_flow_logs(flow_log_file, lookup_table)
    write_output(tag_counts, port_protocol_counts, output_file)
