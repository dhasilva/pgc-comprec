import time, pickle, matplotlib.pyplot as plt
from sklearn.linear_model import Perceptron
from sklearn.linear_model import SGDClassifier
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.externals import joblib

start_all = time.time()

def get_seconds(start):
    end = time.time()
    elapsed = str(round(end - start))
    return elapsed
    
def log(line):
    f = open('./training.log', 'a')
    f.write(line + '\n')
    f.close()
    print(line)

### Treinamento

batch_size = 1000
n_pairs = 516000
train_set_size = 510000
n_batches = int(train_set_size / batch_size)
classes = [0, 1]

classifiers = [
    ("clfP", Perceptron(penalty = None)),
    ("clfPL1", Perceptron(penalty = 'l1')),
    ("clfSGDCL1", SGDClassifier(learning_rate = 'constant', penalty = 'l1', loss = 'hinge', eta0 = 1)),
    ("clfSGDO", SGDClassifier(learning_rate = 'optimal', penalty = 'l2', loss = 'hinge')), # SGD()
    ("clfSGDOL1", SGDClassifier(learning_rate = 'optimal', penalty = 'l1', loss = 'hinge')),
    ("clfSGDOLOG", SGDClassifier(learning_rate = 'optimal', penalty = 'l2', loss = 'log', eta0 = 1)),
    ("clfPAH", PassiveAggressiveClassifier(loss = 'hinge')),
    ("clfPASH", PassiveAggressiveClassifier(loss = 'squared_hinge'))
    # Removidos:
    #("clfPL2", Perceptron(penalty = 'l2')),
    #("clfSGDC", SGDClassifier(learning_rate = 'constant', penalty = 'l2', loss = 'hinge', eta0 = 1)),
    #("clfSGDCLOG", SGDClassifier(learning_rate = 'constant', penalty = 'l2', loss = 'log', eta0 = 1)),
]

log('n_pairs: ' + str(n_pairs) + ' train_set_size: ' + str(train_set_size) + ' n_batches: ' + str(n_batches) + ' batch_size: ' + str(batch_size))

for j in range(n_batches):
    start_batch = time.time()
    X = pickle.load(open('.\Batches\X.' + str(j), 'rb'))
    y = pickle.load(open('.\Batches\y.' + str(j), 'rb'))
    for name, clf in classifiers:
        clf.partial_fit(X, y, classes)
        joblib.dump(clf, '.\Classificadores\\' + name + '\\' + str(j) + '.clf', compress = 3)
    log('[' + get_seconds(start_all) + ' | ' + str(round(100 * (j + 1) / n_batches)) + '%] - Batch ' + str(j + 1) + ' processada em ' + get_seconds(start_batch) + ' segundos')

log(str(n_batches) + ' batches processadas em ' + get_seconds(start_all) + ' segundos')

### Testes

log('Iniciando os testes.')
n_batches_test = int(n_pairs / batch_size) - n_batches
results = {}
# Inicializando os resultados em 0
for clf_name, clf in classifiers:
    results[clf_name] = []
    for i in range(n_batches):
        results[clf_name].append({
            'tp': 0,
            'tn': 0,
            'fp': 0,
            'fn': 0,
            'i': i
        })

for j in range(n_batches, int(n_pairs / batch_size)):
    start_batch = time.time()
    Xt = pickle.load(open('.\Batches\X.' + str(j), 'rb'))
    yt = pickle.load(open('.\Batches\y.'+ str(j), 'rb'))
    # Para cada classificador
    for clf_name, clf in classifiers:
        path = '.\Classificadores\\' + clf_name + '\\'
        # Para cada versão do classificador
        for i in range(n_batches):
            clf = joblib.load(path + str(i) + '.clf')
            yp = clf.predict(Xt)
            tp = results[clf_name][i]['tp']
            tn = results[clf_name][i]['tn']
            fp = results[clf_name][i]['fp']
            fn = results[clf_name][i]['fn']
            for k in range(len(yt)):
                if yt[k] == yp[k]:
                    if yt[k] == 0:
                        tn = tn + 1
                    else:
                        tp = tp + 1
                else:
                    if yt[k] == 0:
                        fp = fp + 1
                    else:
                        fn = fn + 1
            results[clf_name][i]['tp'] = tp
            results[clf_name][i]['tn'] = tn
            results[clf_name][i]['fp'] = fp
            results[clf_name][i]['fn'] = fn
            if(tp + fp != 0):
                results[clf_name][i]['precision'] = tp/(tp+fp)
            else:
                results[clf_name][i]['precision'] = -1
            if(tp+fn != 0):
                results[clf_name][i]['recall'] = tp/(tp+fn)
            else:
                results[clf_name][i]['recall'] = -1
            results[clf_name][i]['mean_accuracy'] = (tp+tn)/(tp+tn+fp+fn)
    # Salva os resultados em um arquivo
    save_path = '.\Classificadores\\'
    pickle.dump(results, open(save_path + 'results.pkl', 'wb'))
    completion = str( round( 100 * (j - n_batches + 1) / (int(n_pairs / batch_size) - n_batches) ) ) + '%'
    log('[' + completion + ' - ' + get_seconds(start_all) + ' ' + get_seconds(start_batch) + '] - Batch ' + str(j + 1) + ' processada')
    log('Classificadores finais:')
    for clf_name, clf in classifiers:
        log(clf_name + ' - ' + str(results[clf_name][-1]))

classifiers = [
    ('clfP', 'Perceptron', 'r-'),
    ('clfPL1', 'Perceptron L1', '#d83e91'),
    ('clfSGDCL1', 'SGD L1 hinge Const', 'k--'),
    ('clfSGDO', 'SGD L2 hinge Opti', 'g-'),
    ('clfSGDOL1', 'SGD L1 hinge Opti', 'g--'),
    ('clfSGDOLOG', 'SGD L2 log Opti', 'y--'),
    ('clfPAH', 'PA hinge', 'b--'),
    ('clfPASH', 'PA squared_hinge', '#958bf3')
]

# Plota os resultados em um gráfico
example_axis = [x*2000 for x in range(1, n_batches + 1)]
plt.figure(figsize=(30, 21), dpi=80)
plt.title('Acurácia média em função do número de exemplos')
plt.xlabel('Exemplos')
plt.ylabel('Acurácia')
plt.grid(True)
for clf_name, clf_desc, line_color in classifiers:
    plt.plot(example_axis, [y['mean_accuracy'] for y in results[clf_name]], line_color, label = clf_desc)
plt.axis([example_axis[0], example_axis[-1], 0, 1])
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.savefig('resultadosAcurácia.png', bbox_inches='tight')

plt.figure(figsize=(30, 21), dpi=80)
plt.title('Revocação em função do número de exemplos')
plt.xlabel('Exemplos')
plt.ylabel('Revocação')
plt.grid(True)
for clf_name, clf_desc, line_color in classifiers:
    plt.plot(example_axis, [y['recall'] for y in results[clf_name]], line_color, label = clf_desc)
plt.axis([example_axis[0], example_axis[-1], 0, 1])
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.savefig('resultadosRevocação.png', bbox_inches='tight')

plt.figure(figsize=(30, 21), dpi=80)
plt.title('Precisão em função do número de exemplos')
plt.xlabel('Exemplos')
plt.ylabel('Precisão')
plt.grid(True)
for clf_name, clf_desc, line_color in classifiers:
    plt.plot(example_axis, [y['precision'] for y in results[clf_name]], line_color, label = clf_desc)
plt.axis([example_axis[0], example_axis[-1], 0, 1])
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.savefig('resultadosRevocação.png', bbox_inches='tight')
