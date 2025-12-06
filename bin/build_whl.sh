rm -rf ./dist
rm -rf ./sisi_ops.egg-info

conda activate sisi
python -m build .