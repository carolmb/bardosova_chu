import glob

files = glob.glob('subset_chu/citation_*_180122_bardo_infomap_220322.xnet')


for file in files:
    name = file[24:-33].title()
    print(name)