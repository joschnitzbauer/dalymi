'''
This example showcases a few dalymi concepts with a simplistic data science pipeline.
Note that the simplicity of the algorithm would not necessarily require a pipeline nor is it rocket science,
we merely use it to convey dalymi oncepts.
Check the comments below to follow the details.
'''
import logging

from dalymi import Pipeline, PipelineCLI
from dalymi import resources
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans


# dalymi pipeline operations log on level INFO. Hence, the following causes dalymi to be verbose:
logging.basicConfig()
logging.getLogger('dalymi').setLevel(logging.INFO)


#
# Custom resource classes
#


class FigureResource(resources.LocalFileMixin, resources.Resource):
    '''
    A resource that stores matplotlib figures. All custom resources must inherit from `resources.Resource`.
    `resources.LocalFileMixin` provides the `check` and `delete` method for simple local files.
    We never want to load figures, so the only thing left to implement is the `save` method.
    '''
    def save(self, path, figure):
        return figure.savefig(path)


#
# Pipeline resources
#


# Raw data as acquired from the source
raw = resources.PandasCSV(name='raw',
                          loc='data/raw.csv',
                          columns=['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])

# Pre-processed data ready for our machine learning algorithm
prepared = resources.PandasCSV(name='prepared',
                               loc='data/prepared.csv',
                               columns=['sepal_length', 'sepal_width', 'petal_length', 'petal_width'])

# A pickled scikit-learn model
# `{clusters}` will be replaced during runtime by the value of a `--clusters` command line argument.
model = resources.Pickle(name='model',
                         loc='data/clusters={clusters}/model.pkl')

# The pre-processed data with an additional predicted cluster column
predictions = resources.PandasCSV(name='predictions',
                                  loc='data/clusters={clusters}/predictions.csv',
                                  columns=prepared.columns + ['cluster'])

# A grid plot of cluster assignment by feature pairs
plot = FigureResource(name='clusters',
                      loc='data/clusters={clusters}/plot.png')


#
# Pipeline definition
#


pl = Pipeline()


@pl.output(raw)
def get_data(**context):
    ''' Retrieves raw data from the internet. '''
    return pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data',
                       header=None, names=raw.columns)


@pl.output(prepared)
@pl.input(raw)
def prep_data(raw, **context):
    ''' Cleans raw data by dropping columns and applies standard scaling to the features. '''
    # drop class column as we will do unsupervised clustering
    prepared = raw.drop('class', axis=1)
    # standard-scale features
    scaler = StandardScaler()
    prepared.loc[:, prepared.columns] = scaler.fit_transform(prepared)
    return prepared


@pl.output(model)
@pl.input(prepared)
def train_model(prepared, clusters, **context):
    ''' Trains a k-means clustering algorithm given a number of `clusters`. '''
    model = KMeans(n_clusters=clusters)
    model.fit(prepared)
    return model


@pl.output(predictions)
@pl.input(model, prepared)
def predict_clusters(model, prepared, **context):
    ''' Predicts cluster assigment using the trained model. '''
    prepared['cluster'] = model.predict(prepared)
    return prepared


@pl.output(plot)
@pl.input(predictions)
def plot_clusters(predictions, **context):
    ''' Generates a grid of scatter plots to visualize cluster assignment. '''
    figure = plt.figure(figsize=(20, 16))
    figure.suptitle('Predicted clusters')
    for i in range(4):
        for j in range(4):
            axes = figure.add_subplot(4, 4, 4 * i + j + 1)
            axes.scatter(predictions.iloc[:, i], predictions.iloc[:, j], c=predictions['cluster'])
            axes.set_xlabel(predictions.columns[i] + ' (scaled)')
            axes.set_ylabel(predictions.columns[j] + ' (scaled)')
    return figure


if __name__ == '__main__':
    # Instead of simply calling `pl.run()`, we create a CLI object which allows
    # finer-control of command line arguments:
    cli = PipelineCLI(pl)

    # We add an argument to the run command, so that we can specify the number of k-means clusters:
    cli.run_parser.add_argument('-c', '--clusters',
                                type=int,
                                default='3',
                                help='the number of clusters')

    # Parses command line arguments, adds them to the default context and runs the pipeline:
    cli.run()
