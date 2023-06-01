#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong

# Python program to detect cycle
# in a graph
from collections import defaultdict


class Graph():
    """
    只支持 整数类型 的节点标识
    """

    def __init__(self, vertices):
        self.graph = defaultdict(list)
        self.V = vertices

    def addEdge(self, u, v):
        self.graph[u].append(v)

    def isCyclicUtil(self, v, visited, recStack):
        # Mark current node as visited and
        # adds to recursion stack
        visited[v] = True
        recStack[v] = True
        # Recur for all neighbours
        # if any neighbour is visited and in
        # recStack then graph is cyclic
        for neighbour in self.graph[v]:
            if visited[neighbour] == False:
                if self.isCyclicUtil(neighbour, visited, recStack) == True:
                    return True
            elif recStack[neighbour] == True:
                return True
        # The node needs to be poped from
        # recursion stack before function ends
        recStack[v] = False
        return False

    # Returns true if graph is cyclic else false
    def isCyclic(self):
        visited = [False] * self.V
        recStack = [False] * self.V
        for node in range(self.V):
            if visited[node] == False:
                if self.isCyclicUtil(node, visited, recStack) == True:
                    return True
        return False


class GraphTraverse(object):
    """
    主要是做图遍历，可以和上面的类合并，但是没必要
    """
    def __init__(self):
        self.graph = defaultdict(list)

    def addEdge(self, u, v):
        self.graph[u].append(v)

    def graph_traverse(self,start_node):

        # 循环体
        view_nodes = []
        # 终止条件
        into_nodes = self.graph.get(start_node, None)
        if into_nodes is not None:

            view_nodes.extend(into_nodes)
            for start_node in into_nodes:
                child_view_nodes = self.graph_traverse(start_node)
                view_nodes.extend(child_view_nodes)

        return view_nodes

    def graph_traverse_distinct(self, start_node):
        view_nodes = self.graph_traverse(start_node)

        view_sets = set()
        for view_node in view_nodes:
            view_sets.add(view_node)

        return view_sets

    @staticmethod
    def static_graph_traverse(graph,start_node):

        # 循环体
        view_nodes = []
        # 终止条件
        into_nodes = graph.get(start_node, None)
        if into_nodes is not None:

            view_nodes.extend(into_nodes)
            for start_node in into_nodes:
                child_view_nodes = GraphTraverse.static_graph_traverse(graph,start_node)
                view_nodes.extend(child_view_nodes)

        return view_nodes

    @staticmethod
    def static_graph_traverse_distinct(graph,start_node):
        view_nodes = GraphTraverse.static_graph_traverse(graph,start_node)

        view_sets = set()
        for view_node in view_nodes:
            view_sets.add(view_node)

        return view_sets

if __name__ == '__main__':
#
#     g = Graph(5)
#     # g.addEdge(0, 0)
#     g.addEdge('0', '0')
#     # g.addEdge(1, 0)
#     # g.addEdge(0, 2)
#     # g.addEdge(1, 2)
#     # g.addEdge(2, 0)
#     # g.addEdge(2, 3)
#     # g.addEdge(3, 3)
#     if g.isCyclic() :
#         print("Graph has a cycle")
#     else:
#         print("Graph has no cycle")



    graph = {'A': ['B', 'C'],
             'C': ['D', 'E','F'],
             'D': ['F', 'K'],
             'H': ['A', 'K'],
             }


    # 从开始的

    start_node = 'A'

    view_nodes = GraphTraverse().static_graph_traverse(graph,start_node)
    print(view_nodes)

    view_nodes = GraphTraverse().static_graph_traverse_distinct(graph,start_node)
    print(view_nodes)


    graph_t = GraphTraverse()
    graph_t.addEdge('A','B')
    graph_t.addEdge('A','C')
    graph_t.addEdge('C','D')
    graph_t.addEdge('C','E')
    graph_t.addEdge('C','F')
    graph_t.addEdge('D','F')
    graph_t.addEdge('D','K')
    view_nodes = graph_t.graph_traverse(start_node)
    print(view_nodes)

    view_nodes = graph_t.graph_traverse_distinct(start_node)
    print(view_nodes)