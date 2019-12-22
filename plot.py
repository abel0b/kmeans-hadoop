import matplotlib.pyplot as plt
import argparse
import numpy

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="input data")
    parser.add_argument("columns", help="columns number")
    args = parser.parse_args()

    usecols = list(map(int, args.columns.split(",")))
    print(usecols)
    data = numpy.loadtxt(
        fname=args.input_file,
        delimiter=",",
        usecols=usecols
    )

    print(data)
    """
    x = [1.0,12.0,78.0,14.0,45.0,13.0,69.0]
    y = [0.0,0.0,0.0,0.0,0.0,0.0,0.0]
    clusters = [1, 2, 3, 2, 1, 1, 3]
    
    plt.scatter(x, y, c=clusters)

    plt.show()
    """ 
