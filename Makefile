lib: clean
	mkdir python

	# move source to python/
	rsync -amr --include="*.py" --include="*/" --exclude="*" ./src python/
	rsync -amr --include="*.py" --include="*/" --exclude="*" ./lambda_handlers python/
	
	# package into layer.zip
	zip -r lib.zip python

	# delete python folder
	rm -rf python

pyenv: clean
	mkdir python

	# install requirements
	python3 -m pip install --upgrade -r requirements.txt -t python/
	
	# package into layer.zip
	zip -r pyenv.zip python

	# delete python folder
	# rm -rf python

clean:
	rm -rf python
	rm -f lib.zip pyenv.zip

pyclean:
	find . -type d -name __pycache__ -exec rm -r {} \; 2>/dev/null


