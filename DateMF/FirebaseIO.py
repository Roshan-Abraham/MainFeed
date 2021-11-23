import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
cred = credentials.Certificate('key.json')
try:    
    firebase_admin.initialize_app(cred)
except:
    pass
db = firestore.client()


class FireBase:

    user_data = {}

    def __init__(self):

        # self.db = firestore.client()
        self.user_data["Drinking"] = 'NA'
        self.user_data["Smoking"] = 'NA'
        self.user_data["Gender"] = 'NA'
        self.user_data["Name"] = 'NA'
        self.user_data["Id"] = 'NA'
        self.user_data["SexPreference"] = 'NA'
        self.user_data["Age"] = 'NA'
        self.user_data["PAgeMax"] = 'NA'
        self.user_data["PAgeMin"] = 'NA'

    def GetD(self):
        # db = firestore.client()
        self.users_ref = db.collection(u'root')
        self.docs = self.users_ref.stream()
        for doc in self.docs:
            self.user_data["Drinking"] = u'{}'.format(doc.to_dict()['Drinking'])
            self.user_data["Smoking"] = u'{}'.format(doc.to_dict()['Smoking'])
            self.user_data["Gender"] = u'{}'.format(doc.to_dict()['Gender'])
            self.user_data["Name"] = u'{}'.format(doc.to_dict()['Name'])
            # self.user_data["Id"] = u'{}'.format(doc.id)
        return(self.user_data)

    def DateGet(self, id):
        self.user_ref = db.collection(u'DateUsers').document(id)
        self.doc = self.user_ref.get()

        tagData = {}
        tagData["city"] = u'{}'.format(self.doc.to_dict()['city'])
        
        # for doc in docs:
        self.user_data["Drinking"] = u'{}'.format(self.doc.to_dict()['drinking'])
        self.user_data["Smoking"] = u'{}'.format(self.doc.to_dict()['smoking'])
        self.user_data["Gender"] = u'{}'.format(self.doc.to_dict()['gender'])
        self.user_data["Age"] = u'{}'.format(self.doc.to_dict()['age'])
        self.user_data["PrefDrinking"] = u'{}'.format(self.doc.to_dict()['pDrinking'])
        self.user_data["PrefSmoking"] = u'{}'.format(self.doc.to_dict()['pSmoking'])
        self.user_data["SexPreference"] = u'{}'.format(self.doc.to_dict()['wishToMeet'])
        self.user_data["AgeRangeS"] = u'{}'.format(self.doc.to_dict()['preffAge']['start'])
        self.user_data["AgeRangeE"] = u'{}'.format(self.doc.to_dict()['preffAge']['end'])

        #count of existing documents in users BestMatches
        # collectBM = db.collection('DateUsers').document(id).collection('BestMatches')
        # collectionLen = len(list(collectBM.get()))
        # #count of existing documents in users Swiped sub-collection
        # collectSM = db.collection('DateUsers').document(id).collection('Swiped')
        # collectionLen = collectionLen + len(list(collectSM.get()))

        #list existing uuid in sub-collections bestmatches
        CurrentQueryBM = db.collection('DateUsers').document(id).collection('BestMatches')
        CurrentListBM = [docls.id for docls in CurrentQueryBM.stream()]

        CurrentQuerySM = db.collection('DateUsers').document(id).collection('Swiped')
        CurrentList = [docls.id for docls in CurrentQuerySM.stream()]
        # CurrentListSM = []
        # CurrentListBM = []
        CurrentList.append(CurrentListBM)


        # unfit = db.collection('DateUsers').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
        if self.user_data["SexPreference"] == 'both':
            # print('Entering first if case')
            all_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).where(u'smoking', u'==', self.user_data["PrefSmoking"]).where(u'drinking', u'==', self.user_data["PrefDrinking"]).limit(len(CurrentList) + 400)
            all_user = [doc.to_dict() for doc in all_users.stream()]
            # all_id = [doc.id for doc in all_users.stream()]

            df = pd.DataFrame.from_dict(all_user)
            # df['id'] = all_id

            df = df[~df['uuid'].isin(CurrentList)]

            if len(df) < 400:
                filler_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).limit(len(CurrentList) + 400)
                filler_user = [doc.to_dict() for doc in filler_users.stream()]
                PreCalledList = df['uuid'].tolist()
                filler_user = filler_user[~filler_user['uuid'].isin(PreCalledList)]
                filler_user = filler_user[~filler_user['uuid'].isin(CurrentList)]

                df = df.append(filler_user)

            df = df[0:400]

            return df, tagData
            # print('exit if case')
        else:

            all_users = db.collection('DateUsers').where(u'gender', u'==', self.user_data["SexPreference"]).where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).where(u'smoking', u'==', self.user_data["PrefSmoking"]).where(u'drinking', u'==', self.user_data["PrefDrinking"]).limit(len(CurrentList) + 400)
            all_user = [doc.to_dict() for doc in all_users.stream()]
            # all_id = [doc.id for doc in all_users.stream()]

            df = pd.DataFrame.from_dict(all_user)
            # df['id'] = all_id

            df = df[~df['uuid'].isin(CurrentList)]
        
            if len(df) < 400:
                filler_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).limit(len(CurrentList) + 400)
                filler_user = [doc.to_dict() for doc in filler_users.stream()]
                PreCalledList = df['uuid'].tolist()
                filler_user = pd.DataFrame.from_dict(filler_user)
                filler_user = filler_user[~filler_user['uuid'].isin(PreCalledList)]
                filler_user = filler_user[~filler_user['uuid'].isin(CurrentList)]

                df = df.append(filler_user)
                
            df = df[0:400]

            return df, tagData
            # print('exit else case')


    def DateGetTest(self, id):
        self.user_ref = db.collection(u'DateUsers').document(id)
        self.doc = self.user_ref.get()

        tagData = {}
        tagData["city"] = u'{}'.format(self.doc.to_dict()['city'])
        
        # for doc in docs:
        self.user_data["Drinking"] = u'{}'.format(self.doc.to_dict()['drinking'])
        self.user_data["Smoking"] = u'{}'.format(self.doc.to_dict()['smoking'])
        self.user_data["Gender"] = u'{}'.format(self.doc.to_dict()['gender'])
        self.user_data["Age"] = u'{}'.format(self.doc.to_dict()['age'])
        self.user_data["PrefDrinking"] = u'{}'.format(self.doc.to_dict()['pDrinking'])
        self.user_data["PrefSmoking"] = u'{}'.format(self.doc.to_dict()['pSmoking'])
        self.user_data["SexPreference"] = u'{}'.format(self.doc.to_dict()['wishToMeet'])
        self.user_data["AgeRangeS"] = u'{}'.format(self.doc.to_dict()['preffAge']['start'])
        self.user_data["AgeRangeE"] = u'{}'.format(self.doc.to_dict()['preffAge']['end'])

        #count of existing documents in users BestMatches
        # collectBM = db.collection('DateUsers').document(id).collection('BestMatches')
        # collectionLen = len(list(collectBM.get()))
        # #count of existing documents in users Swiped sub-collection
        # collectSM = db.collection('DateUsers').document(id).collection('Swiped')
        # collectionLen = collectionLen + len(list(collectSM.get()))

        #list existing uuid in sub-collections bestmatches
        CurrentQueryBM = db.collection('DateUsers').document(id).collection('BestMatches')
        CurrentListBM = [docls.id for docls in CurrentQueryBM.stream()]

        CurrentQuerySM = db.collection('DateUsers').document(id).collection('Swiped')
        CurrentList = [docls.id for docls in CurrentQuerySM.stream()]
        # CurrentListSM = []
        # CurrentListBM = []
        CurrentList.append(CurrentListBM)


        # unfit = db.collection('DateUsers').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
        if self.user_data["SexPreference"] == 'both':
            # print('Entering first if case')
            all_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).where(u'smoking', u'==', self.user_data["PrefSmoking"]).where(u'drinking', u'==', self.user_data["PrefDrinking"]).limit(len(CurrentList) + 400)
            all_user = [doc.to_dict() for doc in all_users.stream()]
            # all_id = [doc.id for doc in all_users.stream()]

            df = pd.DataFrame.from_dict(all_user)
            # df['id'] = all_id

            df = df[~df['uuid'].isin(CurrentList)]

            # if len(df) < 400:
            #     filler_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).limit(len(CurrentList) + 400)
            #     filler_user = [doc.to_dict() for doc in filler_users.stream()]
            #     PreCalledList = df['uuid'].tolist()
            #     filler_user = filler_user[~filler_user['uuid'].isin(PreCalledList)]
            #     filler_user = filler_user[~filler_user['uuid'].isin(CurrentList)]

            #     df = df.append(filler_user)

            df = df[0:400]

            return df, tagData
            # print('exit if case')
        else:

            all_users = db.collection('DateUsers').where(u'gender', u'==', self.user_data["SexPreference"]).where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).where(u'smoking', u'==', self.user_data["PrefSmoking"]).where(u'drinking', u'==', self.user_data["PrefDrinking"]).limit(len(CurrentList) + 400)
            all_user = [doc.to_dict() for doc in all_users.stream()]
            # all_id = [doc.id for doc in all_users.stream()]

            df = pd.DataFrame.from_dict(all_user)
            # df['id'] = all_id

            df = df[~df['uuid'].isin(CurrentList)]
        
            # if len(df) < 400:
            #     filler_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).limit(len(CurrentList) + 400)
            #     filler_user = [doc.to_dict() for doc in filler_users.stream()]
            #     PreCalledList = df['uuid'].tolist()
            #     filler_user = pd.DataFrame.from_dict(filler_user)
            #     filler_user = filler_user[~filler_user['uuid'].isin(PreCalledList)]
            #     filler_user = filler_user[~filler_user['uuid'].isin(CurrentList)]

            #     df = df.append(filler_user)
                
            df = df[0:400]

            return df, tagData

    def DateGetLite(self, id):
        self.user_ref = db.collection(u'DateUsers').document(id)
        self.doc = self.user_ref.get()

        tagData = {}
        tagData["city"] = u'{}'.format(self.doc.to_dict()['city'])
        
        # for doc in docs:
        # self.user_data["Drinking"] = u'{}'.format(self.doc.to_dict()['drinking'])
        # self.user_data["Smoking"] = u'{}'.format(self.doc.to_dict()['smoking'])
        # self.user_data["Gender"] = u'{}'.format(self.doc.to_dict()['gender'])
        # self.user_data["Age"] = u'{}'.format(self.doc.to_dict()['age'])
        # self.user_data["PrefDrinking"] = u'{}'.format(self.doc.to_dict()['pDrinking'])
        # self.user_data["PrefSmoking"] = u'{}'.format(self.doc.to_dict()['pSmoking'])
        self.user_data["SexPreference"] = u'{}'.format(self.doc.to_dict()['wishToMeet'])
        self.user_data["AgeRangeS"] = u'{}'.format(self.doc.to_dict()['preffAge']['start'])
        self.user_data["AgeRangeE"] = u'{}'.format(self.doc.to_dict()['preffAge']['end'])

        #count of existing documents in users BestMatches
        # collectBM = db.collection('DateUsers').document(id).collection('BestMatches')
        # collectionLen = len(list(collectBM.get()))
        # #count of existing documents in users Swiped sub-collection
        # collectSM = db.collection('DateUsers').document(id).collection('Swiped')
        # collectionLen = collectionLen + len(list(collectSM.get()))

        #list existing uuid in sub-collections bestmatches
        CurrentQueryBM = db.collection('DateUsers').document(id).collection('BestMatches')
        CurrentListBM = [docls.id for docls in CurrentQueryBM.stream()]

        CurrentQuerySM = db.collection('DateUsers').document(id).collection('Swiped')
        CurrentList = [docls.id for docls in CurrentQuerySM.stream()]
        # CurrentListSM = []
        # CurrentListBM = []
        CurrentList.append(CurrentListBM)


        # unfit = db.collection('DateUsers').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
        if self.user_data["SexPreference"] == 'both':
            # print('Entering first if case')
            all_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).limit(len(CurrentList) + 400)
            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]

            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id

            df = df[~df['id'].isin(CurrentList)]
            df = df[0:400]

            return df, tagData
            # print('exit if case')
        else:

            all_users = db.collection('DateUsers').where(u'gender', u'==', self.user_data["SexPreference"]).where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).limit(len(CurrentList) + 400)
            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]

            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id

            df = df[~df['id'].isin(CurrentList)]
            df = df[0:400]

            return df, tagData
    


    def XGet(self, id):
        self.user_ref = db.collection(u'DateUsers').document(id)
        self.doc = self.user_ref.get()
        # for doc in docs:
        self.user_data["Drinking"] = u'{}'.format(self.doc.to_dict()['drinking'])
        self.user_data["Smoking"] = u'{}'.format(self.doc.to_dict()['smoking'])
        self.user_data["Gender"] = u'{}'.format(self.doc.to_dict()['gender'])
        self.user_data["Age"] = u'{}'.format(self.doc.to_dict()['age'])
        self.user_data["PrefDrinking"] = u'{}'.format(self.doc.to_dict()['pDrinking'])
        self.user_data["PrefSmoking"] = u'{}'.format(self.doc.to_dict()['pSmoking'])
        self.user_data["SexPreference"] = u'{}'.format(self.doc.to_dict()['wishToMeet'])
        self.user_data["AgeRangeS"] = u'{}'.format(self.doc.to_dict()['preffAge']['start'])
        self.user_data["AgeRangeE"] = u'{}'.format(self.doc.to_dict()['preffAge']['end'])

        # unfit = db.collection('DateUsers').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
        if self.user_data["SexPreference"] == 'both':
            print('Entering first if case')
            all_users = db.collection('DateUsers').where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).where(u'Smoking', u'==', self.user_data["PrefSmoking"]).where(u'Drinking', u'==', self.user_data["PrefDrinking"]).limit(2)
            print('exit if case')
        else:

            all_users = db.collection('DateUsers').where(u'gender', u'==', self.user_data["SexPreference"]).where(u'age', u'>=', int(self.user_data['AgeRangeS'])).where(u'age', u'<=', int(self.user_data['AgeRangeE'])).where(u'Smoking', u'==', self.user_data["PrefSmoking"]).where(u'Drinking', u'==', self.user_data["PrefDrinking"]).limit(2)
            print('exit else case')

        all_user = [doc.to_dict() for doc in all_users.stream()]
        # all_id = [doc.id for doc in all_users.stream()]
        # # all_id = []
        # # for docx in all_users.stream():
        # #     all_id = f'{docx.id} => {docx.to_dict()}' 

        df = pd.DataFrame.from_dict(all_user)
        # # dfid = pd.DataFrame.from_dict(all_id)
        # df['id'] = all_id

        return df
        

    def UGet(self, id):
        self.user_ref = db.collection(u'DateUsers').document(id)
        self.docs = self.user_ref.get()
        # for doc in docs:
        self.user_data["Drinking"] = u'{}'.format(self.docs.to_dict()['Drinking'])
        self.user_data["Smoking"] = u'{}'.format(self.docs.to_dict()['Smoking'])
        self.user_data["Gender"] = u'{}'.format(self.docs.to_dict()['gender'])
        # self.user_data["Name"] = u'{}'.format(self.docs.to_dict()['n'])

        self.user_data["SexPreference"] = u'{}'.format(self.docs.to_dict()['Wish to Meet'])
        self.user_data["Age"] = u'{}'.format(self.docs.to_dict()['age'])
        self.user_data["PAgeMax"] = u'{}'.format(self.docs.to_dict()['Preferred partner max age'])
        self.user_data["PAgeMin"] = u'{}'.format(self.docs.to_dict()['Preferred partner min age'])

        # unfit = db.collection('DateUsers').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
        if self.user_data["SexPreference"] == 'both':
            print('Entering first if case')
            all_users = db.collection('DateUsers').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
            print('exit if case')
        else:

            all_users = db.collection('DateUsers').where(u'Gender', u'==', self.user_data["SexPreference"]).where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
            print('exit else case')

        all_user = [doc.to_dict() for doc in all_users.stream()]
        all_id = [doc.id for doc in all_users.stream()]
        # all_id = []
        # for docx in all_users.stream():
        #     all_id = f'{docx.id} => {docx.to_dict()}' 

        df = pd.DataFrame.from_dict(all_user)
        # dfid = pd.DataFrame.from_dict(all_id)
        df['id'] = all_id

        return df
        
    def SGet(self, id):
        self.user_ref = db.collection(u'DateUsers').document(id)
        self.docs = self.user_ref.get()
        # for doc in docs:
        self.user_data["Drinking"] = u'{}'.format(self.docs.to_dict()['Drinking'])
        self.user_data["Smoking"] = u'{}'.format(self.docs.to_dict()['Smoking'])
        self.user_data["Gender"] = u'{}'.format(self.docs.to_dict()['Gender'])
        # self.user_data["Name"] = u'{}'.format(self.docs.to_dict()['n'])

        self.user_data["SexPreference"] = u'{}'.format(self.docs.to_dict()['Wish to Meet'])
        self.user_data["Age"] = u'{}'.format(self.docs.to_dict()['Age'])
        self.user_data["PAgeMax"] = u'{}'.format(self.docs.to_dict()['Preferred partner max age'])
        self.user_data["PAgeMin"] = u'{}'.format(self.docs.to_dict()['Preferred partner min age'])

        unfit = db.collection('DateUsers').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"]).limit(5)

        # x = f'{self.docs.to_dict()}'
        # x = [doc.id for doc in unfit.stream()]
        x = [doc.to_dict() for doc in unfit.stream()]
        # x = pd.DataFrame.from_dict(x)
        return x

    def SendD(self, cluster, id):
        self.users_refx = db.collection(u'root').document(
            id).collection(u'OnBdCluster')
        self.users_refx.set({u'ClusterNumber': cluster}, merge=True)
        return 'ClusterID uploaded'

def MCFunc():
    x = FireBase()
    see = x.GetD()
    # print(see)
    return see


def EAFunc(persona, id):
    x = FireBase()
    x.SendD(persona, id)

def DGet(id, smoke):
    FireBase.GetD()
    all_users = db.collection('DummyMLAIHardik').where(u'g', u'==', u'Male').where(u's', u'==', smoke)#.where(u'Drinking', u'==', drink)

    all_users = [doc.to_dict() for doc in all_users.stream()]

    df = pd.DataFrame.from_dict(all_users)

def SendDocDate(x,id):
    for i in x:
        db.collection(u'DateUsers').document(id).collection(u'BestMatches').document(i['id']).set(i)
    field_updates = {"bestMatchesCount": len(x)}
    db.collection('DateUsers').document(id).update(field_updates)
    return 'MFList uploaded'

def DyUpdate(x, id):
    new_user = db.collection(u'DateUsers').document(id).get()
    new_user_id = u'{}'.format(new_user.id)
    new_user= f'{new_user.to_dict()}'
    # print(type(x))
    # print(new_user)
    # for i in x:
    #     print(i)
    db.collection(u'DateUsers').document(x).collection(u'MainFeed').document(new_user_id).set(new_user)
    