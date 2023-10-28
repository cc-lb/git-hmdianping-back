package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.nio.file.CopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class   UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    UserMapper userMapper;


    /**
     *  登录
     *  1.检验手机号码和校验码
     *  2.查看用户是否存在，未存在则直接创建用户
     *  3.redis中存入用户信息，为校验用户状态做准备
     */

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {

        //1.首先检验手机号码格式是否正确
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone))
            return  Result.fail("手机号码错误");

        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);

        //1.检验验证码是否错误
        if(!cacheCode.equals(loginForm.getCode()))
            return Result.fail("验证码错误");


        //2.查找用户信息，无用户则自动注册
        //User user = userMapper.selectOne(new LambdaQueryWrapper<User>().eq(User::getPhone, phone));
        User user = query().eq("phone", phone).one();


        if(user==null){

            user = createUserWithPhone(phone);
        }
        System.out.println("新增用户"+user);
        //toString（true）选择32位
        String token = UUID.randomUUID().toString(true);

        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);//关于BeanUtil和BeanUtils
       // UserDTO userDTO = new UserDTO();
        //BeanUtils.copyProperties(user,userDTO);


        //3.存储用户信息
        //为什么存储token，检验登录状态。
        //对于存储用户数据结构采用hash，可以指定删除value中的字段的值
        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>()
                , CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor(
                            (fieldName,fieldValue)->
                        {if(fieldValue==null)  fieldValue="0";
                            else  fieldValue=fieldValue.toString();
                           return fieldValue;} ));



       String tokenKey= LOGIN_USER_KEY+token;
       stringRedisTemplate.opsForHash().putAll(tokenKey, map);
       // stringRedisTemplate.opsForValue().set(tokenKey,LOGIN_USER_TTL, TimeUnit.MINUTES);

        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL,TimeUnit.MINUTES);



        // HashMap<String, UserDTO> map= new HashMap<>();


        return Result.ok(token);
    }

    @Override
    public Result sign() {
        UserDTO user = UserHolder.getUser();
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter
                .ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + user.getId() + keySuffix;
        int dayOffset = now.getDayOfMonth();
        stringRedisTemplate.opsForValue()
                .setBit(key, dayOffset-1, true);
        return  Result.ok();



    }

    @Override
    public Result signCount() {
        UserDTO user = UserHolder.getUser();
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter
                .ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + user.getId() + keySuffix;
        int dayOffset = now.getDayOfMonth();
        // 获取本月截止今天为止的所有的签到记录，返回的是一个十进制的数字 BITFIELD sign:5:202203 GET u14 0
        List<Long> field = stringRedisTemplate.opsForValue()
                .bitField(key,
                        BitFieldSubCommands.create()
                                //bit图位置结束
                                .get(BitFieldSubCommands.BitFieldType.unsigned(dayOffset))
                                //bit图位置开始
                                .valueAt(0)
                );
        if(field==null|| field.isEmpty()) return  Result.ok(0);

        Long num = field.get(0);

        if (num == null || num == 0) {
            return Result.ok(0);
        }
        // 6.循环遍历
        int count = 0;
        while (true) {
            // 6.1.让这个数字与1做与运算，得到数字的最后一个bit位  // 判断这个bit位是否为0
            if ((num & 1) == 0) {
                // 如果为0，说明未签到，结束
                break;
            }else {
                // 如果不为0，说明已签到，计数器+1
                count++;
            }
            // 把数字右移一位，抛弃最后一个bit位，继续下一个bit位
            num >>>= 1;
        }
        return Result.ok(count);

    }

    private User createUserWithPhone(String phone) {
        // 1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // 2.保存用户
        save(user);
        return user;
    }
}
