# -*- coding:utf-8 -*-

if __name__ == '__main__':
    """
    fi = open('data/hashtag.txt')
    f = open('data/no_hashtag.txt', 'w')
    count = 1
    for line in fi.readlines():
        f.write(str(count) + '\t'+ line)
        count += 1
    print count
    f.close()
    fi.close()

    """
    fi = open('data/weibo.txt')
    f = open('data/no_weibo.txt', 'w')
    count = 1
    for line in fi.readlines():
        f.write(str(count) + '\t' + line)
        count += 1
    print count
    f.close()
    fi.close()


