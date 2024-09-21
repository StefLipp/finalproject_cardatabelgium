import io
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print('fix up unstructured header')
    
    #drop columns for municipality index
    data_transf = data.drop(['Index','Index.1','Index.2','Index.3','Index.4'], axis=1)
    
    #define new header and drop top column
    new_header = data_transf[data_transf.Rang == 'Rang'].values.tolist()[0]
    data_transf = data_transf[data_transf.Rang != 'Rang']
    data_transf.columns = new_header

    #erase decimals

    data_transf = data_transf.to_csv(index=False)
    return pd.read_csv(io.StringIO(data_transf))

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'