# BlockchainArbitration
> Репозиторий разработки программного обеспечения, предназначенный для сбора данных и анализа курсов криптовалютных бирж с целью поиска арбитражных возможностей для торговли
![alt text](https://github.com/Tronnert/crypto_7_12_22/blob/main/docs/meme.png?raw=true)
<br/>

## Пояснение
**Цель** работы - изучить явление арбитражной торговли на рынке криптовалют с помощьюю анализа больших данных
Поставленные **задачи**:
1. Создать тееоретическую базу проекта: ознакомление с существующими токенами, устройство ордеров, виды арбитражных возможностей и т.п.
2. Собрать исторические данные о покупах и продахаж криптовалют из нескольких разных источников для дальнейшего анализа
3. Провести разведочный анализ, определить наиболее оптимальный способ взаимодействия c данными на каждом этапе 
4. Проанализировать полученные данные, посчитать метрики
5. Построить графики на основе анализа
6. Сделать выводы

## Структура проекта

```
C:.
|   consts.py
|   functions.py
|   get_period.py
|   main.py
|   scheduler.py
|             
+---analysis
|   |   avg_duration.ipynb
|   |   real_revenue.py
|   |   revenue_file.py
|   |   revenue_graphs.ipynb
|   |   save_graphs_images.py
|   |   time.py
|   |   
|   |       
|   +---revenue
|   |   part-00000.csv
|   |
|   \---time_output
|       part-00000.csv
|           
|       
+---images
|       binance_only_profitable.png ...
|       
+---json
|       binance_sub.json
|       bitget_sub.json
|       bybit_sub.json
|       different_names.json
|       exchange_fee.json
|       gate_io_sub.json
|       huobi_sub.json
|       kraken_sub.json
|       poloniex_sub.json
|       
+---logs
|       logs.tsv ...
|       
\---sockets
       base_websocket.py
       binance_websocket.py
       bitget_websocket.py
       bybit_websocket.py
       gate_websocket.py
       huobi_websocket.py
       kraken_websocket.py
       poloniex_websocket.py
```
```consts.py``` - файл с множеством констант (в основном, пути к файлам) 

```functions.py``` - файл с различными функциями, которые не стали методами классов (рисование графиков и обработка некоторых запросов)

```get_period.py``` - файл, запускаемый с параметрами в терминале для получения исторических данных за промежуток времени

```main.py``` - тот же get_period, только для ручного запуска и без длителньости записи (запись будет идти, пока есть место на устройстве)

```scheduler.py``` - файл с планировщиком расписания, который периодически вызывает запись в логи данных с подчиненных ему бирж

```sockets``` - папка с файлами классов вебсокетов каждой используемов нами бирж; ```base_websocket.py``` - родительский класс

```images``` - папка, куда будут сохранятся графики

```logs``` - папка, куда сохраняются логи

```json``` - папка с важнными json-файлами, в основном, со скелетами сообщений, которые отправляем на api бирж для получения данных

```analysis```- папка, содержащая файлы для анализа исторический данных

```revenue_file.py``` - транформация, удаление и размножение исторических данных с целью нахождения прибыли; сохраняет преобразованные данные в новый датасет

```time.py``` - создание datamart по среднему времени арбитражной ситуации в разрезе пары символов и пары бирж

```save_graphs_images.py``` - нужен для построения и сохранения графиков (графики распределения прибыли по биржам и графики для наиболее прибыльных символов)

 ```revenue_graphs.ipynb``` и ```avg_duration.ipynb``` - ноутбуки с графиками

В ```real_revenue.py``` строится datamart по модели получения реальной прибыли с данных 


## Как работать с проектом?

Схему работы пользователя с проектом можно обознаачить следующим образом:
> Получение исторических данных -> подсчет метрик -> построение графиков

Получение исторических данных осуществляется либо использованием main-файла (осторожно, в нем данные получаются бесконечно), либо использованием get_period_файла
Вызов в терминале получения исторических данных:

```python3 get_period.py --filename [filename] --duration [duration] --include [exch1, eexch2, ...] --exclude [exch1, exch2, ...]```

Длительность получения данных указывается в секундах. 
В include можно перечислить те биржи, с которых надо брать данные (по умолчанию со всех)
В exclude перечисляются те биржи, данные с которых не надо собирать (по умолчанию таких нет)
Также есть параметр ```--progress_bar```, выводящий шкалу выполнения сбора даннных (при использовании шкалы не выводятся ошибки при получении данных с бирж)

Все даннные сохраняются в папке *logs*

Подсчет метрик осуществляется в файлах ```revenue_file.py``` и ```time.py```. Для их корректного вызова надо менять путь открытия файла на соответствующий интересующим логам. После выполнения программ в папке analysis будут находится две подпапки с датасетами, применяющимия для построения графиков

Для построения графиков используйте блокноты в папке analysis, указав там путь к датасетам в созданных ранее подпапках

В тексте не написать всех особенностей использования проекта (их много), поэтому для решения вопросов обращайтесь к разработчикам
