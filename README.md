# How do I set up?

* Make sure that you use `python3.8` or above
* Inside a virtualenv            : `pip3 install streamAPI -U`
* If you want to install globally: `sudo -H pip3 install streamAPI -U`
* To run test: `python -m unittest discover -s test/ -p '*_test.py'` or `make test`

## Philosophy

We, in day to day data handling, have to usually setup pipeline to process
data, that can be transforming data, filtering it, processing it sequentially
or concurrently, reducing it and so on. StreamAPI attempts to provide solution to such problem in
clean and easy way and enforces readability. 
