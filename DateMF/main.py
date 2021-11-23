# from FirebaseIO import SendD
import os
from flask import Flask, request, jsonify
# from firebase_admin import credentials, firestore, initialize_app
# from google.cloud import storage
from ModelPred import PredCluster
from FirebaseIO import FireBase, SendDocDate
# import pandas as pd
from datetime import date

app = Flask(__name__)

def dateDiff(startDate):
    diff = date.today() - startDate.date()
    if diff.days >= 31:
        return True
    else:
        return False

@app.route('/')
def entry():
    return 'DateUser BestMatches entry point API'


@app.route('/test')
def entrytest():
    id = request.args.get('id')
#Get list of filtered users/ in DataFrame format
    y = FireBase()
    df, temper = y.DateGet(id)
    temper = 0
    df = df.to_json(orient ='records')
    df = df.replace("true", "True")
    df = df.replace("false", "False")
    df = eval(df)
    return str(df) #str(type(df)), 200
#function for OrderScore based on plan
def order(plan, score):
    return ((score) - 4) if plan == 'celeb' else( ((score) - 6) if plan == 'elite' or 'limelight' else ((score) - 10))


@app.route('/datemf', methods=['GET','POST'])
def MFlist():
    # try:

#Identify Anchor (Main) User 
    id = request.args.get('id')
#Get list of filtered users/ in DataFrame format
    y = FireBase()
    df, tagData = y.DateGet(id)

    
    dfcp = df[['personalityTraits', 'interests']]
    dfcp.rename(columns = {'personalityTraits':'Personality Traits', 'interests':'Interests'}, inplace = True)

    # df = pd.DataFrame.from_dict(all_users)
    # df = df.drop('Name')
    # df.set_index("Name", inplace=True)
    out = PredCluster(dfcp)
    outdf = df.loc[out.index]
    
    #Convert value to percentage scale and limiting with -10 to control plan improvement
    outdf['compScore'] = int((out*100) - 10)

    outdf['orderScore'] = int(outdf[['plan','ComScore']].apply(lambda x: order(*x), axis=1))


    #Insied Tags sub nested doc
    #add field likesYou : False
    #function for nerby location
    #logic currently based on same city of users instead of location radius 
    outdf['nearby'] = outdf['city'].apply(lambda x: True if x == tagData['city'] else False)

    outdf['new'] = outdf['createdAt'].apply(lambda x: dateDiff(x))


    outdf = outdf.to_json(orient ='records')
    outdf = outdf.replace("true", "True")
    outdf = outdf.replace("false", "False")
    # bestMatchDF = outdf[['uuid','name','age','designation','isVerified','dpUrl','imgUrls','prompts','OrderScore','ComScore','interests','personalityTraits','new','nearby']]
    bestMatchDF = eval(outdf)
    SendDocDate(bestMatchDF, id)    # SendDes(outdf, id)
    
    return 'Docs uploaded', 200
    # except Exception as e:
    #     return f"An Error Occured: {e}"  


@app.route('/datemflite', methods=['GET','POST'])
def MFlistlite():
    # try:

#Identify Anchor (Main) User 
    id = request.args.get('id')
    if not id:
        msg = "no id received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400       
#Get list of filtered users/ in DataFrame format
    y = FireBase()
    df, tagData = y.DateGetLite(id)

    
    dfcp = df[['personalityTraits', 'interests']]
    dfcp.rename(columns = {'personalityTraits':'Personality Traits', 'interests':'Interests'}, inplace = True)

    # Sending Only 2 columns 'Personality Traits' & 'Interests' to DateMainFeed model in ModelPred scipt for preprocessing and prediction
    out = PredCluster(dfcp)

    #Adding output score from model(array) to existing Dataframe to reduce procing in mocving large amounts of data
    #index is static and unchanging between reading and  model prediction
    outdf = df.loc[out.index]
    
    #Convert value to percentage scale and limiting with -10 to control plan improvement
    outdf['ComScore'] = (out*100) - 10

    #Add field for ordering list of users to view based on the users purchased plans
    outdf['OrderScore'] = outdf[['plan','ComScore']].apply(lambda x: order(*x), axis=1)

    #function for nerby location
    #logic currently based on same city of users instead of location radius 
    #value in boolean format
    outdf['nearby'] = outdf['city'].apply(lambda x: True if x == tagData['city'] else False)

    #Unsure how logic will hold up  for users in bottom of list who will be viewed late
    #compare date of creation from current date
    #value in boolean format
    outdf['new'] = outdf['createdAt'].apply(lambda x: dateDiff(x))


    outdf = outdf.to_json(orient ='records')
    #converting boolean values into lower caps for firebase purposes 
    outdf = outdf.replace("true", "True")
    outdf = outdf.replace("false", "False")
    # bestMatchDF = outdf[['uuid','name','age','designation','isVerified','dpUrl','imgUrls','prompts','OrderScore','ComScore','interests','personalityTraits','new','nearby']]
    
    #formating dict for uploading into firestoer DB
    bestMatchDF = eval(outdf)

    #Uploading function
    SendDocDate(bestMatchDF, id)
    NoDocs = len(bestMatchDF)
    return f'Docs uploaded {NoDocs}', 200


port = int(os.environ.get('PORT', 8080))
if __name__ == '__main__':
    app.run(debug=True, threaded=True, host='0.0.0.0', port=port)