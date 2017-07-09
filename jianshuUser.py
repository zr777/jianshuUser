# -------------------数据库 (------------------- #
# http://5izxy.cn/2016/03/28/MongoDB/
# http://api.mongodb.com/python/current/faq.html
# mongodb可视化选用 RoboMongo  # db.getCollection('users_7_9').find({}).count()
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.jianshu
coll = db.users_7_9
# -------------------数据库 )------------------- #

import requests
# from requests.compat import urljoin
import pandas as pd
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import time
start = time.ctime()

from multiprocessing.dummy import Pool
pool = Pool(30)

host = 'http://www.jianshu.com'
ua = UserAgent()
headers = {'User-Agent': ua.chrome}

# ---------------------收集当前所有推荐作者-------------------------
def get_recommend_users():
    url = host + '/recommendations/users?page=1&per_page=1000'
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'lxml')
    recommend_users_id = [
        i.find('a')['href'].split('/')[-1]
        for i in soup.select(".wrap")
    ]
    print("共得到 %s 名推荐作者" % len(recommend_users_id))
    return recommend_users_id

def get_info_from_single_user(id):
    url = host + '/u/{0}'.format(id)  # url_user_single = 'http://www.jianshu.com/u/ddae1fe6d804'
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'lxml')
    name = soup.select_one('.title .name').text.strip()
    print('Done %s' % name)
    info = soup.select(".info li")
    return {
        'id': id,
        'name': name,
        'following': info[0].find('p').text,  # 'following_url': urljoin(host, info[0].find('a')['href']),
        'followers': info[1].find('p').text,  # 'followers_url': urljoin(host, info[1].find('a')['href']),
        'articles': info[2].find('p').text,
        'words': info[3].find('p').text,
        'likes': info[4].find('p').text,
        'order': 0, 
    }

def get_recommend_users_info():
    recommend_users_id = get_recommend_users()
    full_info = pool.map(
        get_info_from_single_user, recommend_users_id)
    return full_info

recommend_users_info = get_recommend_users_info()


# ---------------------得到推荐作者的关注用户-------------------------
from retry import retry
# pip install retry
# https://pypi.python.org/pypi/retry
@retry(Exception, delay=1, backoff=2, tries=2)
def get_followers_or_following(id, order=1, state='following'):
    # 无论网页还是程序访问都是最多900个，像简叔这样关注超2000个的无法全部获取
    url = host + '/users/{id}/{state}?page=%s'.format(id=id, state=state)
    users = []
    num = 1
    while 1:
        res = requests.get(url%num, headers=headers)
        soup = BeautifulSoup(res.text, 'lxml')
        follow_users = soup.select(".user-list .info")
        if not follow_users:
            break
        for i in follow_users:
            spans = i.findAll('span')
            des = i.findAll('div')[-1].text.split(' ')
            href = i.find('a')['href']
            users.append({
                'id': href.split('/')[-1],
                'name': i.find('a').text.strip(),
                'following': spans[0].text.split(' ')[-1],
                'followers': spans[1].text.split(' ')[-1],
                'articles': spans[2].text.split(' ')[-1],
                'words': des[7],
                'likes': des[9],
                'order': order,
            })
        # print(num)
        num += 1
    return users

# from threading import Lock
# lock = Lock()
# users = [] # https://stackoverflow.com/questions/6319207/are-lists-thread-safe
# with lock:
# users.extend(new_users)

errors = []

def get_next_layer_users_through_following(info, store=False):
    try:
        new_users = get_followers_or_following(
            info['id'], order=info['order']+1)
        if store and new_users:
            coll.insert_many(new_users)
        print('--- ', info['name'])
        return new_users
    except:
        errors.append(info)
        print('error', info['id'], info['name'])
        return []

second_layer_users = pool.map(
    get_next_layer_users_through_following, recommend_users_info)
# [[dict1, dict2], [dict3, dict4], ...]  --> [dict1, dict2, dict3, dict4, ...]
print('第二层 即推荐作者的关注 完成')
from itertools import chain
users = [i for i in chain(*second_layer_users, recommend_users_info)]
users = pd.DataFrame(users)
users.drop_duplicates('id', inplace=True, keep='last')

from functools import partial
third_layer_users = pool.map(
    partial(get_next_layer_users_through_following, store=True),
    users[users['order']>0].to_dict('records'))

end = time.ctime()
print('start', start, '; end', end)
print('出错数为', len(errors))

# --------------------处理出错的连接 (------------------------ #
if len(errors) != 0:
    errors_ = errors
    errors = []
    repair = pool.map(
        partial(get_next_layer_users_through_following, store=True),
        errors_)
    print('再次出错数为', len(errors))
# --------------------处理出错的连接 )------------------------ #

pool.close()
pool.join()


# --------------------  数据处理  -------------------------------
more_users = [i for i in chain(*third_layer_users)]
more_users = pd.DataFrame(more_users) # 564620
del more_users['_id'] # 因为插入数据库所以会存在_id列
more_users.drop_duplicates('id', inplace=True) # 128306

all_users = pd.concat([users, more_users])
all_users.drop_duplicates('id', inplace=True, keep='first')
# 清洗后存入新的数据库
coll = db.users_clean
coll.insert_many(all_users.to_dict('records'))

# 读取使用
# cursor = coll.find({}, {'_id': False})
# document = [d for d in cursor]
# all_users = pd.DataFrame(document)#, dtype=int

columns = ['articles', 'followers', 'following', 'likes', 'words', 'order']
all_users[columns] = all_users[columns].astype('int')

exception_users = all_users[ all_users['likes']<0 ]
# 去除没有喜欢数的用户并排序
all_users = all_users[ all_users['likes']>0 ].sort_values(
    ['likes', 'followers'], ascending=False
).reset_index(drop=True)

# 查看我的信息
me = all_users[all_users['name'].str.contains('treelake')]
all_users[all_users['likes'] > me['likes'].item()]

print('喜欢数大于1000的所有用户数为 --> ',
      len(all_users[all_users['likes']>1000]))

# 利用matplotlib库可视化喜欢数
import matplotlib.pyplot as plt
all_users['likes'].plot.line()
plt.show()
# 利用sns美化matplotlib可视化喜欢数密度分布
import seaborn as sns
sns.distplot(all_users[all_users['likes']<10000]['likes'])
sns.plt.show()
sns.distplot(all_users[all_users['likes']>10000]['likes'])
sns.plt.show()
# 查看相关性
sns.jointplot(data=all_users, x='words', y='likes', kind='reg', color='g')
sns.jointplot(data=all_users, x='articles', y='likes', kind='reg', color='g')
sns.plt.show()

# 使用动态可视化库 Plotly
import plotly
import plotly.figure_factory as ff
import plotly.graph_objs as go

# 画表格
table = ff.create_table(all_users[:100])
plotly.offline.plot(table, filename='jianshu_user_table1.html')

# 柱状图
filtered = all_users[all_users['likes']>100]

for d, t in zip(
    ['likes', 'words', 'articles', 'followers', 'following', 'order'],
    ['喜欢数', '总字数', '文章数', '关注ta的人数', '被ta关注的人数', '爬取层级']):
    fig = {
        'data': [go.Bar(x=filtered.name,
                        y=filtered[d])],
        'layout': {'yaxis': {'title': t}},
    }
    plotly.offline.plot(fig, filename='basic_bar_%s.html'%d, show_link=False)


# 散点图
trace1 = go.Scatter(
    x = filtered.words,
    y = filtered.likes,
    mode='markers',
    marker=dict(
        size='16',
        color=filtered.followers.apply(pd.np.sqrt), #set color equal to a variable
        colorscale='Viridis',
        # Greys, YlGnBu, Greens, YlOrRd, Bluered, RdBu, Reds, Blues,
        # Picnic, Rainbow, Portland, Jet, Hot, Blackbody, Earth, Electric, Viridis
        showscale=True,
    ),
    text=filtered.name,
)
data = [trace1]
plotly.offline.plot(data, filename='scatter-plot-with-colorscale.html', show_link=False)


# 点大小不一, 对数坐标轴的散点图
trace1 = go.Scatter(
    x = filtered.words,
    y = filtered.likes,
    mode='markers',
    marker=dict(
        size=filtered.articles.apply(pd.np.sqrt), # 3/(filtered.order + 1),
        color=filtered.followers.apply(pd.np.sqrt), # set color equal to a variable
        colorscale='Rainbow',
        showscale=True,
    ),
    text=filtered.name,
)
fig = {
    'data': [trace1],
    'layout': {
        'xaxis': {'title': '总字数', 'type': 'log'},
        'yaxis': {'title': "总喜欢数", 'type': 'log'},
    },
}
plotly.offline.plot(fig, filename='scatter-plot-with-colorscale22.html', show_link=False)
