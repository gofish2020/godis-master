package logger

import (
	"fmt"
	"os"
)

func checkNotExist(src string) bool {
	_, err := os.Stat(src)
	return os.IsNotExist(err)
}

func checkPermission(src string) bool {
	_, err := os.Stat(src)
	return os.IsPermission(err)
}

func isNotExistMkDir(src string) error {
	if checkNotExist(src) {
		return mkDir(src)
	}
	return nil

}

func mkDir(src string) error {
	return os.MkdirAll(src, os.ModePerm)
}

func mustOpen(fileName, dir string) (*os.File, error) {

	// *校验目录权限
	if checkPermission(dir) {
		return nil, fmt.Errorf("permission denied dir: %s", dir)
	}

	// *判断目录是否存在 + 创建目录（这里其实可以直接创建目录，即可）
	if err := isNotExistMkDir(dir); err != nil {
		return nil, fmt.Errorf("error during make dir %s, err: %s", dir, err)
	}

	// *打开文件（不存在会自动创建），O_APPEND 追加写（权限 读/写）
	f, err := os.OpenFile(dir+string(os.PathSeparator)+fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("fail to open file, err: %s", err)
	}

	return f, nil
}
