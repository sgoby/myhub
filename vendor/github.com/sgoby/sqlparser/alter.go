package sqlparser

import (
	"strings"
	"fmt"
)

type Alter struct {
	Qualifier string
	TableName string
	Action    string
	Content   string
}

func ParseAlterStmt(query string) (*Alter, error) {
	pAlter := &Alter{}
	flagIndex := strings.Index(query,";")
	if flagIndex >= 0{
		query = query[:flagIndex]
	}
	query = strings.Replace(query, "`", "", -1)
	query = strings.Replace(query, "\n", "", -1)
	query = strings.ToLower(query)
	tokens := strings.Split(query, " ")
	if len(tokens) < 4 || tokens[0] != AlterStr {
		return pAlter, fmt.Errorf("Parse alter faild. %s", query)
	}
	//
	pAlter.TableName = tokens[2]
	pAlter.Action = tokens[3]
	//
	if len(tokens) > 4 {
		pAlter.Content = strings.Join(tokens[4:], " ")
	}
	//
	if strings.Contains(pAlter.TableName, ".") {
		dt := strings.Split(pAlter.TableName, ".")
		if len(dt) > 1 {
			pAlter.Qualifier = dt[0]
			pAlter.TableName = dt[1]
		}
	}
	return pAlter, nil
}

//
func (*Alter) iStatement() {}
func (node *Alter) Format(buf *TrackedBuffer) {
	if len(node.Qualifier) > 1 {
		buf.Myprintf("alter table %s.%s %s %s", node.Qualifier, node.TableName, node.Action, node.Content)
		return
	}
	buf.Myprintf("alter table %s %s %s", node.TableName, node.Action, node.Content)
}
func (node *Alter) WalkSubtree(visit Visit) error {
	return nil
}
