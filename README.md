# IOMgr
An asynchronous event handler for the SDS data-path.

## Brief
To build (assuming a recent version of conan package manager is installed)
```
   $ conan create . <user>/<channel>
```
To publish:
```
   # You'll need credentials for this!
   $ conan upload -r origin iomgr/<version>@/<user>/<channel>
```
Official builds can be found under the **user** ``sds`` in either the ``stable`` or ``testing`` channels.
```
   $ conan search -r origin iomgr*
```
