package pattern

const (
	normal         = 0
	all            = 1 // *
	any            = 2 // ?
	symbolSet      = 3 // [...]
	symbolRange    = 4 // [a-b]
	symbolNegative = 5 // [^a]
)

type Pattern struct {
	items []*item
}

type item struct {
	symbol byte          // symbol to match
	set    map[byte]bool // symbols to match
	mode   int           // match mode
}

func (i *item) containsSymbol(symbol byte) bool {
	// match in symbol set
	if i.mode == symbolSet {
		_, ok := i.set[symbol]
		return ok
	} else if i.mode == symbolRange {
		// match symbol range
		var max uint8 = 0
		var min uint8 = 255
		for s, _ := range i.set {
			if s > max {
				max = s
			}
			if s < min {
				min = s
			}
		}
		return symbol <= max && symbol >= min
	} else {
		// match symbol not in set
		_, ok := i.set[symbol]
		return !ok
	}
}

// ParsePattern parses pattern string, gets a match item list
func ParsePattern(p string) *Pattern {
	items := make([]*item, 0)
	isSet := false
	var set map[byte]bool
	for _, v := range p {
		ch := byte(v)
		// set mode to match all
		if ch == '*' {
			items = append(items, &item{mode: all})
		} else if ch == '?' {
			// set mode to match any
			items = append(items, &item{mode: any})
		} else if ch == '[' {
			// check if set has been created
			if !isSet {
				isSet = true
				set = make(map[byte]bool)
			} else {
				set[ch] = true
			}
		} else if ch == ']' {
			if isSet {
				mode := symbolSet
				if _, ok := set['-']; ok {
					// range set, delete '-'
					delete(set, '-')
					mode = symbolRange
				}
				if _, ok := set['^']; ok {
					// negative set, delete '^'
					delete(set, '^')
					mode = symbolNegative
				}
				items = append(items, &item{mode: mode, set: set})
				isSet = false
			} else {
				// ']' as a normal symbol for matching
				items = append(items, &item{mode: normal, symbol: ch})
			}
		} else {
			if isSet {
				set[ch] = true
			} else {
				items = append(items, &item{mode: normal, symbol: ch})
			}
		}
	}
	return &Pattern{items: items}
}

/*
Matches function tells if a key is matched to the pattern

h?llo
h[...]llo

hello
*/
func (p *Pattern) Matches(key string) bool {
	m := len(key)
	n := len(p.items)
	// dp[i][j] 表示key的前i个字符与p的前j个item是否匹配
	dp := make([][]bool, m+1)
	for i := 0; i < m+1; i++ {
		dp[i] = make([]bool, n+1)
	}
	dp[0][0] = true
	for j := 1; j < n+1; j++ {
		dp[0][j] = dp[0][j-1] && p.items[j-1].mode == all
	}

	for i := 1; i < m+1; i++ {
		for j := 1; j < n+1; j++ {
			// 目前item匹配all
			if p.items[j-1].mode == all {
				// 前i个字符在前j个item是否匹配 = 前一个字符匹配（把当前字符给 * ） 或者 当前字符在前一个item匹配（把字符i给前一个item，不给*）
				dp[i][j] = dp[i-1][j] || dp[i][j-1]
			} else {
				dp[i][j] = dp[i-1][j-1] &&
					((p.items[j-1].mode == any) ||
						(p.items[j-1].mode == normal && p.items[j-1].symbol == key[i-1]) ||
						(p.items[j-1].mode >= symbolSet && p.items[j-1].containsSymbol(key[i-1])))
			}
		}
	}
	return dp[m][n]
}
