package result

//
type RuleResult struct {
	NodeDB    string
	TbSuffixs []string
}

//
func (this *RuleResult) AddTbSuffix(tbSuffix string) {
	//过滤重复的
	for _,ts := range this.TbSuffixs{
		if ts == tbSuffix{
			return;
		}
	}
	this.TbSuffixs = append(this.TbSuffixs, tbSuffix)
}
func (this *RuleResult) GetNodeDbName() string {
	return this.NodeDB
}
func (this *RuleResult) IsEmpty() bool {
	if len(this.TbSuffixs) < 1 {
		return true
	}
	return false
}
func (this *RuleResult) Equal(r *RuleResult) bool {
	if this.NodeDB != r.NodeDB {
		return false
	}
	if len(this.TbSuffixs) != len(r.TbSuffixs) {
		return false
	}
	for index, suff := range this.TbSuffixs {
		if suff != r.TbSuffixs[index] {
			return false
		}
	}
	return true
}
//去掉重复的TbSuffixs
func (this *RuleResult) RemoveRepTbSuffixs(){
	tempTbSuffixs := []string{}  // 存放结果
	for _,ts := range this.TbSuffixs{
		isFind := false;
		for  _,tempTs := range tempTbSuffixs{
			if ts == tempTs{
				isFind = true;
				break;
			}
		}
		if !isFind{
			tempTbSuffixs = append(tempTbSuffixs,ts)
		}
	}
	this.TbSuffixs = tempTbSuffixs;
}
//
func (this *RuleResult) Intersection(r *RuleResult) (n *RuleResult, ok bool) {
	if this.NodeDB != r.NodeDB {
		return nil, false
	}
	//
	var newSuff []string
	for _, suff := range this.TbSuffixs {
		for _, rsuff := range r.TbSuffixs {
			if suff == rsuff {
				newSuff = append(newSuff, suff)
			}
		}
	}
	//
	if len(newSuff) <= 0 {
		return nil, false
	}
	//
	return &RuleResult{
		NodeDB:    this.NodeDB,
		TbSuffixs: newSuff,
	}, true
}
