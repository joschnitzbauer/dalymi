from dalymi import DAG


dag = DAG()


def first():
    print('Hello', end=' ')


@dag.ensure_dependencies(first)
def second():
    print('world!')


if __name__ == '__main__':
    dag.run(locals())
