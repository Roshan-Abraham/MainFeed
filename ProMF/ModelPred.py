# Pro ModelPred
import pandas as pd
import joblib
from sklearn.preprocessing import MinMaxScaler
from sklearn.feature_extraction.text import CountVectorizer
scaler = MinMaxScaler()


def string_convert(x):

    if isinstance(x, list):
        return ' '.join(x)
    else:
        return x

def vectorization(df, columns):
    column_name = columns[0]
    
    # Checking if the column name has been removed already
    if column_name not in ['Personality Traits', 'Interests', 'Work Goals', 'Investment Goals', 'Hiring Goals', 'Other Goals']:
        return df
    
    if column_name in ['Personality Traits', 'Interests', 'Work Goals', 'Investment Goals', 'Hiring Goals', 'Other Goals']:
        df[column_name.lower()] = pd.Categorical(df[column_name])
        df[column_name.lower()] = df[column_name.lower()].cat.codes
        
        df = df.drop(column_name, 1)
        
        return vectorization(df, df.columns)
    
    else:
        print(column_name)
        vectorizer = CountVectorizer()
        x = vectorizer.fit_transform(df[column_name])
        df_wrds = pd.DataFrame(x.toarray(), columns=vectorizer.get_feature_names())
        new_df = pd.concat([df, df_wrds], axis=1)
        new_df = new_df.drop(column_name, axis=1)

        return vectorization(new_df, new_df.columns)


# user_data = DataFrame of all users who qualify for the search parameters and now can undergo clustering and ranking based on the main usr we are preparing the list for
# user_data1 = it is the df of main (anchor user) user who we are preparing the list for
# user_data1 must belong inside user_data

#currently  usering first name as UID as it a string format to test parameters, cannot test large scale(more than 8-9 users) because first name is not unique identifier
# dont mind the coimmented code, that will play into effect once a  proper UID and anchoring measure is set

def PredCluster(user_data):
    #reference anchor for vectorization to take standard hold
    #not really sure why this doesnt work on single input df
    for col in user_data.columns:
        user_data[col] = user_data[col].apply(string_convert)

    new_cluster= vectorization(user_data, user_data.columns)
    new_cluster.columns=new_cluster.columns.str.lower()
    new_cluster= pd.DataFrame(scaler.fit_transform(new_cluster), index=new_cluster.index, columns=new_cluster.columns)

    model = joblib.load(r"ProMainFeed.joblib")

    cluster_label = model.predict(new_cluster)
    
    new_cluster['Cluster'] = cluster_label
    
    new_list = new_cluster.T.corr()

    user_n = user_data.index[0]
    
    likely_users = new_list[[user_n]].sort_values(by=[user_n],axis=0, ascending=False)[1:100]
    
    return likely_users