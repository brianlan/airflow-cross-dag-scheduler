I have a pandas dataframe looks as follows:

```
df3 = pd.DataFrame({'thekey': ['batch1', 'batch1', 'batch2', 'batch2'] , 'a': ['abc', 'def', 'ace', 'egb'], 'b': ['xxx', 'yyy', 'zzz', 'ppp'], 'state': ['success', 'success', 'failed', 'success']})
```

I want to group by the column `thekey` and get reduced state following the rule:  if the states of all the records with the same `thekey` are 'success', the reduced 'state' is 'success'; otherwise the reduced 'state' is 'failed'. 

Namely, after the operation, I should get the following dataframe:

```
df4 = pd.DataFrame({'thekey': ['batch1', 'batch2'] , 'a': [['abc', 'def'], ['ace', 'egb']], 'b': [['xxx', 'yyy'], ['zzz', 'ppp']], 'state': ['success', 'failed']})
```

Now, my question is, is it achievable? If yes, how should I do this?