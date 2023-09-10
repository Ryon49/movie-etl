default: clean
	mkdir python

	# move source to python/
	rsync -amr --include="*.py" --include="*/" --exclude="*" ./src python/
	
	# install requirements
	python3 -m pip install --upgrade -r requirements.txt -t python/
	
	# package into layer.zip
	zip -r layer.zip python

	# delete python folder
	# rm -rf python

clean:
	rm -rf python
	rm -f layer.zip

pyclean:
	find . -type d -name __pycache__ -exec rm -r {} \; 2>/dev/null


