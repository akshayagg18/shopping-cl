"""Normalize prose spacing while preserving code, URLs and Markdown links."""
import pathlib,re,sys
PROTECTED=re.compile(r'(```.*?```|`[^`]*`|https?://[^\s]+|\[[^\]]*\]\([^)]*\)|\b(?:x86-64-v3|x86_64|linux/amd64|amd64|u8|u16|u32|u64|u128|i32|i64|Ev1|Ev2|Date32|Int64|Int128|P1|P2|p1|p2)\b)',re.S)
def format_text(s):
 parts=PROTECTED.split(s)
 for i in range(0,len(parts),2):
  p=parts[i];p=re.sub(r'(?<=[a-z])(?=[0-9])',' ',p);p=re.sub(r'\bNCPU(\d+)\b',r'NCPU=\1',p)
  p=re.sub(r'(?<=\d)(?=(?:ms|GB/s|GiB|MiB|GB|MB|B|UTC|s)(?:$|[\s/.,;:)\]]))',' ',p)
  p=re.sub(r'(?m)^-(?=\d)','- ',p)
  parts[i]=p
 return ''.join(parts)
if __name__=='__main__':
 for name in sys.argv[1:]:
  p=pathlib.Path(name);p.write_text(format_text(p.read_text()))
