package forest

import (
	"fmt"
	"sync"
	"testing"
)

func TestCachedForest_GetLatest3Ancestors(t *testing.T) {
	ar := []int{1, 2}
	fmt.Println(ar[1 : len(ar)-1])
}

func TestChannelReadAndWrite(t *testing.T) {
	ch := make(chan int, 1)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for v := range ch {
			fmt.Printf("  %d\n", v)
		}
		wg.Done()
		fmt.Printf("\nexit recv channel\n\n")
	}()

	go func() {
		for i := 0; i < 10; i++ {
			ch <- i
		}
		wg.Done()
		// Note, if the channel is not closed, the reading data of the gorountine cannot exit.
		close(ch)

		fmt.Printf("\n exit send channel\n\n")
	}()

	wg.Wait()
}

func TestContinueFor(t *testing.T) {
	ch := make(chan int)
	num := 0
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
	check:
		for i := range ch {
			if i == 3 {
				num++
				fmt.Printf("enter %d\n", num)
				continue check
				//break check
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 100; i++ {
			ch <- i % 10
		}
		wg.Done()
		close(ch)
	}()
	wg.Wait()
}
