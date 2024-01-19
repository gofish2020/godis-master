package sortedset

import (
	"math/bits"
	"math/rand"
	"time"
)

/*
每个层级的节点，都有forward 和span

	if forward == nil {
		span = 当前节点到尾部节点到跨度
	} else if forward != nil {
		span = 当前节点 到 forward 节点到跨度
	}

update 的含义： 限制在skiplist.level层级， 要查找的某个节点的的前驱节点，这个节点 甚至和当前节点没有指向关系
*/
const (
	maxLevel = 16 // 默认最高层级位16
)

// Element is a key-score pair
type Element struct {
	Member string
	Score  float64
}

// Level aspect of a node
type Level struct {
	forward *node // forward node has greater score
	span    int64
}

type node struct {
	Element
	backward *node
	level    []*Level // level[0] is base level
}

type skiplist struct {
	header *node
	tail   *node
	length int64 // 记录其中的节点个数
	level  int16
}

func makeNode(level int16, score float64, member string) *node {

	// 创建新节点
	n := &node{

		// 节点保存的值 score/member
		Element: Element{
			Score:  score,
			Member: member,
		},
		// 节点的层数组
		level: make([]*Level, level),
	}
	// 初始化每一层的Level值
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""), // header节点
	}
}

func randomLevel() int16 {

	// 1 << maxLevel = 2^16 = 65536
	total := uint64(1)<<uint64(maxLevel) - 1 // 0x 00 00 00 00 00 00 FF FF = bits 64位
	rand.Seed(int64(time.Now().Nanosecond()))
	// [0,0xFFFF-0x0001]
	k := rand.Uint64() % total

	// k+1 = [1,0xFFFF] int16(bits.Len64(k+1)) 最小1位，最大16位 maxLevel - int16(bits.Len64(k+1) = [0,15] + 1 = [1,16]
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

func (skiplist *skiplist) insert(member string, score float64) *node {

	update := make([]*node, maxLevel) // link new node with node in `update`
	rank := make([]int64, maxLevel)

	// find position to insert

	// node 表示当前指向的节点
	node := skiplist.header

	// 这个for循环的目的在于不断的【向下移动】
	for i := skiplist.level - 1; i >= 0; i-- {

		/*
			i 表示层高
			rank：表示node节点，在i层的总跨度（即：node距离header的距离）
			update：表示node节点
		*/

		// rank的含义表示从最左边的边界header边界，到当前node的，在i层级上的跨度（这个跨度是从header到node的跨度，总跨度）
		if i == skiplist.level-1 {
			rank[i] = 0
		} else { // for循环再次进入，表示进入到下一层，当前的节点node暂时还是上一层计算的node，所以距离边界header的距离，在当前i层，继承了i+1层
			rank[i] = rank[i+1] // store rank that is crossed to reach the insert position
		}

		// 本质就是查找 member和score应该存放的位置的【前驱节点】（在当前i层）
		if node.level[i] != nil {
			// traverse the skip list

			/*
				继续（当前节点的）下一层的条件为：
				1.当前节点【没有】后缀节点；
				2.当前节点【有】  后缀节点； 分值很大 or 分值相同，但是member比较大


				计算的目的：在于找到前驱节点 node
			*/
			// for循环的目的：在于不断的【向右移动】
			for node.level[i].forward != nil &&
				//后缀节点的分值 偏小
				(node.level[i].forward.Score < score ||
					// 后缀节点分值一样，但是 member 偏小
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) { // same score, different key
				rank[i] += node.level[i].span
				// 当前节点，重新设定为此层i的后面的节点
				node = node.level[i].forward
			}
		}
		// 保存在当前层i的前驱节点
		update[i] = node
	}
	//通过上面的计算，update就是各个层的前驱节点，rank就是各个层的前驱节点的编号

	level := randomLevel() // 随机层高
	// extend skiplist level
	if level > skiplist.level { // 如果新节点的层高比现有的最高还要高
		for i := skiplist.level; i < level; i++ { // 超过的层高部分
			// 前驱节点就是header
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// make node and link into skiplist

	// 这里的node节点就是即将插入的新节点
	node = makeNode(level, score, member)

	// 将新建的节点，插入到链表中
	for i := int16(0); i < level; i++ {

		// 这里用链表的思路理解，每一层进行的节点的插入
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		// rank[0] - rank[i] 表示 update[0]和update[i]两个节点之间的距离
		// update[i].level[i].span  表示 update[i]节点到后驱节点到距离
		// 相减的结果，表示 update[0]节点到后序节点到距离，而插入到node位置恰好就是 update[0]的位置
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])

		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels

	// 如果level的层高比较低，小于最大层高，那么多余的层高（skiplist.level - level) 因为插入了新元素，默认都要自动增减span+1
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// node的backword 之前 前面的节点的节点
	if update[0] == skiplist.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}

	// 将node的后面一个节点的backward指向node
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skiplist.tail = node
	}

	// 因为新插入节点
	skiplist.length++
	return node
}

/*
 * param node: node to delete
 * param update: backward node (of target)
 */

// 要删除节点node，update 保存的是节点node前面的节点列表

func (skiplist *skiplist) removeNode(node *node, update []*node) {

	// 每一层都剔除node节点
	for i := int16(0); i < skiplist.level; i++ {
		if update[i].level[i].forward == node { // 本层有指向关系
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}

	// node是被删除的节点，将后驱指针调整下
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		skiplist.tail = node.backward // 说明当前node是最后一个节点，修改下tail
	}

	// 重新找到层高（最高）
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	// 删掉节点，数量-1
	skiplist.length--
}

/*
 * return: has found and removed node
 */
func (skiplist *skiplist) remove(member string, score float64) bool {
	/*
	 * find backward node (of target) or last node of each level
	 * their forward need to be updated
	 */
	update := make([]*node, maxLevel)
	node := skiplist.header

	// 找到所有的前缀节点（该节点可能和 member并没有直接的指向关系）
	for i := skiplist.level - 1; i >= 0; i-- {

		// 在每个层级，找到满足比member小的第一个节点
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}

		update[i] = node
	}

	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		skiplist.removeNode(node, update)
		// free x
		return true
	}
	return false
}

/*
 * return: 1 based rank, 0 means member not found
 */
func (skiplist *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := skiplist.header

	for i := skiplist.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		if x.Member == member {
			return rank
		}
	}
	return 0
}

/*
 * 1-based rank
 */
func (skiplist *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

func (skiplist *skiplist) hasInRange(min Border, max Border) bool {
	if min.isIntersected(max) { //是有交集的，则返回false
		return false
	}

	// min > tail
	n := skiplist.tail
	if n == nil || !min.less(&n.Element) {
		return false
	}
	// max < head
	n = skiplist.header.level[0].forward
	if n == nil || !max.greater(&n.Element) {
		return false
	}
	return true
}

func (skiplist *skiplist) getFirstInRange(min Border, max Border) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		// if forward is not in range than move forward
		for n.level[level].forward != nil && !min.less(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	/* This is an inner range, so the next node cannot be NULL. */
	n = n.level[0].forward
	if !max.greater(&n.Element) {
		return nil
	}
	return n
}

func (skiplist *skiplist) getLastInRange(min Border, max Border) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	if !min.less(&n.Element) {
		return nil
	}
	return n
}

/*
 * return removed elements
 */
func (skiplist *skiplist) RemoveRange(min Border, max Border, limit int) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)
	// find backward nodes (of target range) or last node of each level
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(&node.level[i].forward.Element) { // already in range
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	// node is the first one within range
	node = node.level[0].forward

	// remove nodes in range
	for node != nil {
		if !max.greater(&node.Element) { // already out of range
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

// 1-based rank, including start, exclude stop
func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0 // rank of iterator
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	// scan from top level
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward // first node in range

	// remove nodes in range
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
