
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Replace fullwidth string
bad_str_start = 'const fullwidth = "'
bad_str_end = '";'
# We can find line by line
lines = content.split("\n")
new_lines = []
for line in lines:
    if "const fullwidth =" in line and ("E" in line or "\ufffd" in line):
        indent = line.split("const")[0]
        new_lines.append(indent + 'const fullwidth = "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ";')
    else:
        new_lines.append(line)

content = "\n".join(new_lines)
with open(path, "w", encoding="utf-8") as f:
    f.write(content)
