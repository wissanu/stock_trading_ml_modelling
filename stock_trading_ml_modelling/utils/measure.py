from sklearn.metrics import roc_auc_score

#Get measures of success
def measure_acc(s_pred,s_act,ft_cols:list=[],opt_text:str='',multiclass:bool=False,cl=None,verbose:bool=False):
    """Function creating key statistical accuracy metrics 
    by comparing predicted and actual series
    -----
    args:
        s_pred - Pandas Series - The values predicted
        s_act - Pandas Series - The actual values
        ft_cols - list - A list of feature columns used
        opt_text - str - A string used to add comments
        verbose - bool - True - Do you want to see the results?
        multiclass - bool - False - If true then perform a one vs many comparisson
        cl - None - The class which is being investigated here
    returns:
        dict - {feature,tot_p,tot_n,tpr,tnr,ppv,npv,acc,auc}
    """
    #Check type of classification
    if multiclass == False:
        true_val = True
    else:
        if cl == None:
            raise ValueError('The value of cl can not be None in a multiclassification situation - please provide a value for cl')
        else:
            true_val = cl
    tot_p = len(s_pred[s_pred == true_val])
    tot_n = len(s_pred[s_pred != true_val])
    #Get tpr, tnr, ppv, npv, and acc
    tp = len(s_pred[(s_pred == true_val) & (s_act == true_val)]) #True positive
    fp = len(s_pred[(s_pred == true_val) & (s_act != true_val)]) #False positive
    tn = len(s_pred[(s_pred != true_val) & (s_act != true_val)]) #True negative
    fn = len(s_pred[(s_pred != true_val) & (s_act == true_val)]) #False negative
    tpr = tp / (tp+fn) if (tp+fn) > 0 else 0
    tnr = tn / (tn+fp) if (tn+fp) > 0 else 0
    ppv = tp / tot_p if tot_p > 0 else 0
    npv = tn / tot_n if tot_n > 0 else 0
    acc = (tp+tn) / (tot_p+tot_n) if tot_n + tot_n > 0 else 0
    #Get roc_auc_score
    bool_act_s = s_act == true_val
    if len(np.unique(bool_act_s)) != 2: #IE nothing is correct
        bool_pred_s = s_pred == true_val
        auc = roc_auc_score(bool_act_s,bool_pred_s)
    else:
        auc = 0.5
    if verbose == True:
        print("\tbool counts act {} -> \n\t\ttrue count:{:,}, false count:{:,}, true %:{:.2f}".format(opt_text,len(s_act[s_act == True]),len(s_act[s_act == False]),100*len(s_act[s_act == True])/len(s_act)))
        print("\tbool counts pred {} -> \n\t\ttrue count:{:,}, false count:{:,}, true %:{:.2f}".format(opt_text,tot_p,tot_n,100*len(s_pred[s_pred == True])/len(s_pred)))
        print("\tdetails {} -> \n\t\t_tp:{:,}, fp:{:,}, tn:{:,}, fn:{:,}".format(opt_text,tp,fp,tn,fn))
        print("\tsummary {} -> \n\t\t_tpr:{:.4f}, tnr:{:.4f}, ppv:{:.4f}, npv:{:.4f}, acc:{:.4f}, auc:{:.4f}".format(opt_text,tpr,tnr,ppv,npv,acc,auc))
    return {
        "feature":ft_cols.copy()
        ,"tot_p":tot_p
        ,"tot_n":tot_n
        ,"tpr":tpr
        ,"tnr":tnr
        ,"ppv":ppv
        ,"npv":npv
        ,"acc":acc
        ,"auc":auc
        ,"opt_text":opt_text
    }