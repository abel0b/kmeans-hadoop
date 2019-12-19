import matplotlib.pyplot as plt

if __name__ == "__main__":
    x = [1.0,12.0,78.0,14.0,45.0,13.0,69.0]
    y = [0.0,0.0,0.0,0.0,0.0,0.0,0.0]
    clusters = [1, 2, 3, 2, 1, 1, 3]
    
    plt.scatter(x, y, c=clusters)

    plt.show()
    


