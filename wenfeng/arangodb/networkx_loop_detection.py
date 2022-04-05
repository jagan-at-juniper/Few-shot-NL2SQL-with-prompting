import networkx as nx

G = nx.DiGraph()

G.add_node("a")
G.add_node("b")
G.add_node("c")
G.add_node("d")
G.add_node("e")

G.add_edge("a", "b")
G.add_edge("b", "c")
G.add_edge("c", "d")
G.add_edge("d", "e")



def check_loop(G):
    try:
        res = nx.find_cycle(G, orientation="original")
        print("loop:", res)
    except Exception as e:
        print("No loop detected")

check_loop(G)

# add loop
G.add_edge("c", "a")
check_loop(G)





