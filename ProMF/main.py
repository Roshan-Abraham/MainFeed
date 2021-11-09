# from FirebaseIO import SendD
import os
from flask import Flask, request
# from firebase_admin import credentials, firestore, initialize_app
# from google.cloud import storage
from ModelPred import PredCluster
from FirebaseIO import FireBase, SendDes
import time
import pandas as pd
from datetime import date


app = Flask(__name__)


#function for OrderScore based on plan
def order(plan, score):
    return ((score) - 4) if plan == 'celeb' else( ((score) - 6) if plan == 'elite' or 'limelight' else ((score) - 10))

def dateDiff(startDate):
    diff = date.today() - startDate.date()
    if diff.days >= 31:
        return True
    else:
        return False


@app.route('/')
def entry():
    return 'ProUser bestMatch API'

@app.route('/promf', methods=['GET','POST'])
def MFlist():
    # try:
    start_time = time.time()
    #Identify Anchor (Main) User 
    id = request.args.get('id')
    #Get list of filtered users/ in DataFrame format
    y = FireBase()
    df, tagData = y.ProGet(id)
    dfcp = df[['personalityTraits', 'interests', 'workGoals', 'investmentGoals', 'hiringGoals', 'otherGoals']]
    dfcp.rename(columns = {'personalityTraits':'Personality Traits', 'interests':'Interests', 'workGoals':'Hiring Goals', 'investmentGoals':'Investment Goals', 'hiringGoals':'Work Goals', 'otherGoals':'Other Goals'}, inplace = True)

    # df = pd.DataFrame.from_dict(all_users)
    # df = df.drop('Name')
    # df.set_index("Name", inplace=True)
    out = PredCluster(dfcp)
    outdf = df.loc[out.index]
    
    #Convert value to percentage scale and limiting with -10 to control plan improvement
    outdf['ComScore'] = (out*100) - 10

    outdf['OrderScore'] = outdf[['plan','ComScore']].apply(lambda x: order(*x), axis=1)

    #function for nerby location
    #logic currently based on same city of users instead of location radius 
    outdf['nearby'] = outdf['city'].apply(lambda x: True if x == tagData['city'] else False)

    outdf['new'] = outdf['createdAt'].apply(lambda x: dateDiff(x))

    outdf = outdf[['uuid','name','age','designation','isVerified','dpUrl','imgUrls','prompts','OrderScore','ComScore','interests','personalityTraits','new','nearby']]
    outdf = outdf.to_json(orient ='records')
    outdf = outdf.replace("true", "True")
    outdf = outdf.replace("false", "False")
    # bestMatchDF = outdf
    bestMatch = eval(outdf)
    SendDes(bestMatch, id)
    NoDocs = len(bestMatch)
    # return ("--- %s seconds ---" % (time.time() - start_time)), 200
    return f'Docs uploaded {NoDocs}', 200

    # except Exception as e:
    #     return f"An Error Occured: {e}"  

@app.route('/oldStable1', methods=['GET','POST'])
def MFovlist():
    # try:

#Identify Anchor (Main) User 
    id = request.args.get('id')
#Get list of filtered users/ in DataFrame format
    y = FireBase()
    df = y.XGet(id)
    dfcp = df[['personality traits', 'interests', 'work_goals', 'investment_goals', 'hiring_goals', 'other_goals']]
    dfcp.rename(columns = {'personality traits':'Personality Traits', 'interests':'Interests', 'work_goals':'Hiring Goals', 'investment_goals':'Investment Goals', 'hiring_goals':'Work Goals', 'other_goals':'Other Goals'}, inplace = True)

    # df = pd.DataFrame.from_dict(all_users)
    # df = df.drop('Name')
    # df.set_index("Name", inplace=True)
    out = PredCluster(dfcp)
    outdf = df.loc[out.index]
    outdf['ComScore'] = out*100
    outdf = outdf.to_json(orient ='records')
    outdf = eval(outdf)
    SendDes(outdf, id)
    
    return str(outdf), 200



port = int(os.environ.get('PORT', 8080))
if __name__ == '__main__':
    app.run(debug=True, threaded=True, host='0.0.0.0', port=port)