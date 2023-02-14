import gdown
import sys

fid = sys.argv[1]
output = sys.argv[2]

url = "https://drive.google.com/uc?id={}&export=download".format(fid)

gdown.download(url, output, quiet=False)