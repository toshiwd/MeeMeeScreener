from scripts import tradex_strict_pit_router_entry_eligibility_v1 as m
def test_gate_contracts():
 assert m.gate_pass('A',1.2,.001,False);assert not m.gate_pass('B',1.2,.001,True);assert not m.gate_pass('C',1.2,.001,False);assert m.gate_pass('C',1.2,.001,True)
def test_only_three_predeclared_gates():assert m.GATES==('A','B','C')
